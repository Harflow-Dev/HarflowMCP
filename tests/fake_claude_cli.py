"""A scripted `claude` executable for registry-level tests of qualification and argv, with no real CLI or API.

`install(directory)` writes `claude` there and returns the directory; put it first on PATH. Behaviour is driven by
environment variables of the *test process* (inherited by the child):

    FAKE_CLAUDE_VERSION       what `claude --version` prints (default 2.1.310)
    FAKE_CLAUDE_INIT_VERSION  `claude_code_version` on the init event; "none" omits it (default 2.1.310)
    FAKE_CLAUDE_EARLY_TOOL    emit a tool_use event before the init event
    FAKE_CLAUDE_ARGV_LOG      append this run's argv as one JSON line
"""

from __future__ import annotations

import stat
import sys
from pathlib import Path

SCRIPT = '''#!{python}
import json, os, sys
if sys.argv[1:] == ["--version"]:
    print(os.environ.get("FAKE_CLAUDE_VERSION", "2.1.310") + " (Claude Code)")
    sys.exit(0)
log = os.environ.get("FAKE_CLAUDE_ARGV_LOG")
if log:
    with open(log, "a") as handle:
        handle.write(json.dumps(sys.argv) + "\\n")
def emit(event):
    event.setdefault("session_id", "fake-session")
    sys.stdout.write(json.dumps(event) + "\\n")
    sys.stdout.flush()
sys.stdin.readline()
if os.environ.get("FAKE_CLAUDE_EARLY_TOOL"):
    emit({{"type": "assistant", "message": {{"content": [{{"type": "tool_use", "id": "toolu_early", "name": "Bash", "input": {{"command": "make test"}}}}]}}}})
init = {{"type": "system", "subtype": "init", "tools": [], "mcp_servers": []}}
version = os.environ.get("FAKE_CLAUDE_INIT_VERSION", "2.1.310")
if version != "none":
    init["claude_code_version"] = version
emit(init)
emit({{"type": "assistant", "message": {{"content": [{{"type": "text", "text": "done"}}]}}}})
emit({{"type": "result", "subtype": "success", "is_error": False, "result": "done", "num_turns": 1, "total_cost_usd": 0.0, "usage": {{}}, "permission_denials": []}})
sys.stdin.read()
'''


def install(directory: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    executable = directory / "claude"
    executable.write_text(SCRIPT.format(python=sys.executable))
    executable.chmod(executable.stat().st_mode | stat.S_IXUSR)
    return directory

#!/usr/bin/env python3
"""No-model Git diagnostics under isolated Codex read-only configuration.

Run outside an existing sandbox. Does not edit personal harness or shell settings.
"""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


def capture(argv, *, env=None):
    try:
        result = subprocess.run(argv, env=env, capture_output=True, text=True, timeout=20)
    except subprocess.TimeoutExpired as error:
        return {"command": argv, "exit": None, "error": f"timed out after {error.timeout}s"}
    return {"command": argv, "exit": result.returncode,
            "stdout": result.stdout, "stderr": result.stderr}


def main():
    if sys.platform != "darwin":
        raise SystemExit("This probe requires macOS and installed Xcode command-line tools.")
    codex = shutil.which("codex")
    if not codex:
        raise SystemExit("Install Codex CLI before running this probe.")
    selection = capture(["/usr/bin/xcode-select", "-p"])
    if selection["exit"] != 0:
        raise SystemExit(json.dumps(selection))
    git = str(Path(selection["stdout"].strip()) / "usr/bin/git")
    with tempfile.TemporaryDirectory(prefix="pb-git-sandbox-") as temporary:
        root = Path(temporary)
        config = root / "codex"
        config.mkdir()
        (config / "config.toml").write_text('approval_policy = "never"\n')
        startup = root / "empty-startup"
        startup.mkdir()
        env = dict(os.environ, CODEX_HOME=str(config))
        cases = [
            ("system_git", ["/usr/bin/git", "--version"], env),
            ("selected_xcode_git", [git, "--version"], env),
            ("login_shell", ["/bin/zsh", "-lc", "command -v git; git --version"], env),
            ("isolated_login_shell", ["/bin/zsh", "-lc", "command -v git; git --version"],
             dict(env, ZDOTDIR=str(startup))),
        ]
        probes = []
        for name, command, case_env in cases:
            result = capture([codex, "sandbox", "-P", ":read-only", "-C", str(root),
                              "--", *command], env=case_env)
            result["case"] = name
            probes.append(result)
        print(json.dumps({"codex": capture([codex, "--version"], env=env),
                          "macos": capture(["/usr/bin/sw_vers"]),
                          "developer_selection": selection, "probes": probes}, indent=2))
    return 0 if all(probe.get("exit") == 0 for probe in probes) else 1


if __name__ == "__main__":
    raise SystemExit(main())

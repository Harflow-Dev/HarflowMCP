#!/usr/bin/env bash
# Install polybridge and register it with the Claude desktop app plus each agent CLI found —
# Claude Code, Codex, opencode.
#
# Deliberately short: you should be able to read an install script before running it.
# Everything fiddly (merging your config, driving each client's own `mcp add`, resolving
# absolute paths) is done by `polybridge-setup`, which is Python and covered by tests.
#
# Takes no arguments. To preview the changes instead, install and then run:
#   polybridge-setup --dry-run
# To register with only some clients:
#   polybridge-setup --client codex,opencode

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

say() { printf '\033[1m==>\033[0m %s\n' "$1"; }
die() { printf '\033[31merror:\033[0m %s\n' "$1" >&2; exit 1; }

# Checked before anything is installed, so a typo can't mutate the machine first.
[ "$#" -eq 0 ] || die "install.sh takes no arguments (got: $*). For options, run polybridge-setup directly."
[ -f "$repo_root/pyproject.toml" ] || die "run this from a polybridge checkout"
[ -n "${HOME:-}" ] || die "HOME is not set; cannot locate your config or user binaries"

# `uv` is ours to install; the agent CLIs are not — they need your account, so setup only looks
# for them and reports which are missing.
if ! command -v uv >/dev/null 2>&1; then
  say "uv not found, installing it from https://astral.sh/uv/install.sh"
  curl -LsSf https://astral.sh/uv/install.sh | sh
  export PATH="${XDG_BIN_HOME:-$HOME/.local/bin}:$PATH"
  command -v uv >/dev/null 2>&1 || die "uv install finished but uv is still not on PATH"
fi

say "installing the server"
# --no-cache is load-bearing: the version never changes, so uv otherwise reuses its cached wheel and
# reinstalls stale code while printing "Installed 1 package".
uv tool install --force --no-cache "$repo_root"

# Asked rather than assumed: this honours UV_TOOL_BIN_DIR and friends.
if tool_bin="$(uv tool dir --bin 2>/dev/null)" && [ -n "$tool_bin" ]; then
  export PATH="$tool_bin:$PATH"
else
  export PATH="${XDG_BIN_HOME:-$HOME/.local/bin}:$PATH"
fi

command -v polybridge-setup >/dev/null 2>&1 \
  || die "installed, but polybridge-setup is not on PATH. Add $(uv tool dir --bin 2>/dev/null || echo "$HOME/.local/bin") to your PATH."

say "registering it with your MCP clients"
# It reports per client and says what to restart, so nothing is repeated here. A non-zero exit means
# at least one client was not *confirmed* — not that it definitely failed: a client whose CLI timed
# out may already have written its config, which is why setup reports that case as `unknown`.
setup_status=0
polybridge-setup || setup_status=$?

if [ "$setup_status" -eq 0 ]; then
  say "done"
else
  printf '\033[31merror:\033[0m not every client was confirmed (see the table above)\n' >&2
fi
exit "$setup_status"

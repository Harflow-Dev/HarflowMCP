#!/usr/bin/env bash
# Install polybridge and register it with the Claude desktop app.
#
# Deliberately short: you should be able to read an install script before running it.
# Everything fiddly (merging your config, resolving absolute paths) is done by
# `polybridge-setup`, which is Python and covered by tests.
#
# Takes no arguments. To preview the config change instead, install and then run:
#   polybridge-setup --dry-run

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

say() { printf '\033[1m==>\033[0m %s\n' "$1"; }
die() { printf '\033[31merror:\033[0m %s\n' "$1" >&2; exit 1; }

# Checked before anything is installed, so a typo can't mutate the machine first.
[ "$#" -eq 0 ] || die "install.sh takes no arguments (got: $*). For options, run polybridge-setup directly."
[ -f "$repo_root/pyproject.toml" ] || die "run this from a polybridge checkout"
[ -n "${HOME:-}" ] || die "HOME is not set; cannot locate your config or user binaries"

# `uv` is ours to install; `claude` is not — it needs your account, so we only check for it.
if ! command -v uv >/dev/null 2>&1; then
  say "uv not found, installing it from https://astral.sh/uv/install.sh"
  curl -LsSf https://astral.sh/uv/install.sh | sh
  export PATH="${XDG_BIN_HOME:-$HOME/.local/bin}:$PATH"
  command -v uv >/dev/null 2>&1 || die "uv install finished but uv is still not on PATH"
fi

say "installing the server"
uv tool install --force "$repo_root"

# Asked rather than assumed: this honours UV_TOOL_BIN_DIR and friends.
if tool_bin="$(uv tool dir --bin 2>/dev/null)" && [ -n "$tool_bin" ]; then
  export PATH="$tool_bin:$PATH"
else
  export PATH="${XDG_BIN_HOME:-$HOME/.local/bin}:$PATH"
fi

command -v polybridge-setup >/dev/null 2>&1 \
  || die "installed, but polybridge-setup is not on PATH. Add $(uv tool dir --bin 2>/dev/null || echo "$HOME/.local/bin") to your PATH."

say "registering it with the Claude desktop app"
polybridge-setup

for agent in claude codex; do
  command -v "$agent" >/dev/null 2>&1 || cat >&2 <<EOF

Note: \`$agent\` is not installed, so its backend will be unusable.
Install it, then re-run ./install.sh so its location gets recorded.

EOF
done

say "done — restart the Claude desktop app to pick up the change"

#!/bin/bash
# Fails if any tracked file, or any untracked file that would be added, refers to a private
# project this repository must not name. Checks file contents and file paths.
#
# Every pattern is assembled from pieces so this script never matches itself.
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 2

name='no''ok'
account='nayandahaberty''personal'
phrase='iOS'' app'
prefix='N''k'

words="(${name}|${account}|${phrase})"
token="(^|[^[:alnum:]_])${prefix}([A-Z][[:alnum:]_]*|([^[:alnum:]_]|$))"

# Build output and untracked caches are never committed; local notes live in .claude/.
excludes=(':!macos/build' ':!.claude' ':!macos/PolybridgeMonitor/.vscode-build' ':!**/.build/**')

status=0

report() {
    echo "check-private-refs: $1" >&2
    status=1
}

if matches=$(git grep --untracked -I -n -i -E "$words" -- . "${excludes[@]}"); then
    echo "$matches" >&2
    report "file contents name a private project"
fi

if matches=$(git grep --untracked -I -n -E "$token" -- . "${excludes[@]}"); then
    echo "$matches" >&2
    report "file contents use a private module prefix"
fi

paths=$( { git ls-files -z; git ls-files -z -o --exclude-standard; } \
    | tr '\0' '\n' \
    | grep -v -E '^(macos/build/|\.claude/|macos/PolybridgeMonitor/\.vscode-build/)' \
    | grep -v -E '(^|/)\.build/' )
if matches=$(printf '%s\n' "$paths" | grep -i -E "$words"); then
    echo "$matches" >&2
    report "file paths name a private project"
fi
if matches=$(printf '%s\n' "$paths" | grep -E "$token"); then
    echo "$matches" >&2
    report "file paths use a private module prefix"
fi

if [ "$status" -eq 0 ]; then
    echo "check-private-refs: clean"
fi
exit "$status"

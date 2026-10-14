# Extra writable paths

`start_task(..., extra_writable_paths=["/absolute/build-output"])` authorizes
additional local writes without selecting unrestricted access. Inspect
`preview_task_permissions(repo_path=..., backend=..., extra_writable_paths=...)`
first: it returns requested/effective typed scopes, certification and enforcement
without launching an agent or allocating task scratch. Launch revalidates this
observation; preview does not reserve authority.

Installed-version qualification runs off the server event loop with a bounded
deadline. Bounded-mode execution uses the qualified absolute executable path;
runtime identity and path checks run again immediately before launch. The effective
repository, scratch directory and extra grants must not overlap the selected
launcher, its resolved executable or any intermediate symlink or lexical ancestor.
These checks precede the version probe and repeat at final validation. Executables
with multiple hardlinks are rejected even when the requested grant is elsewhere:
this conservatively prevents writable indirect aliases without scanning granted
directory trees. Path resolution,
saved-authority reads, runtime qualification and final identity checks run in worker
threads with a 20-second budget per check, including executor queueing. Timeout
refuses the request with an actionable validation error; no agent is launched and
any pre-launch scratch is removed. A filesystem worker can finish after cancellation
or timeout, but performs only authority reads and cannot dispatch an agent. These
checks use a dedicated executor capped at four outstanding workers. Timed-out
workers retain their capacity until they actually finish; saturation refuses new
checks explicitly and leaves the server's shared executor available to unrelated
work. Python cannot interrupt a blocked filesystem syscall, so this budget does
not guarantee immediate process shutdown while such a worker remains blocked.

```sh
polybridge-ctl permission-preview --repo /absolute/repo --backend codex \
  --extra-writable-path /absolute/build-output --json
polybridge-ctl run --repo /absolute/repo --backend codex --prompt 'Build the project' \
  --extra-writable-path /absolute/build-output --json
```

Repeat `--extra-writable-path` for each grant. Grants never authorize committing,
pushing, publishing or remote mutations. Freedom and network authority still apply.

## Measured support

Measured **2026-10-14**, with isolated local fake APIs or installed permission
engines without paid model calls for the certification. One separately authorized
authenticated Antigravity check also confirmed its allowed and denied file-tool
writes, with no observed quota error; its default model identity was not reported.
Only qualified configurations are enabled.

| Harness | Installed version | Evidence and coverage |
| --- | --- | --- |
| Codex | `codex-cli 0.162.0-alpha.17.2`, macOS | Enabled for ordinary exec/resume at `write_in_repo` or `publish`, blocked or enabled network. Directory and existing-file writes succeeded; unrelated writes received OS denials. Fresh continuation uses the same configuration. |
| Claude Code | `2.1.299` | Rejected. Fresh/resume/continue enforce directory file-tool grants, but approved Python shell commands write unrelated paths. Plan mode denies writes; bypass mode is unbounded. Exact-file grants were ineffective. Experimental foreground children inherited file-tool grants; the native adapter is pinned to another version. |
| opencode | `1.19.0` | Rejected. Fresh/session resume enforce file-tool directory rules, but allowed shell scripts bypass them. Exact-file grants, most-recent continuation and native inheritance were not certified. |
| Vibe | `2.26.1` | Rejected. Installed headless permission engine enforces directory/exact-file file-tool rules; allowed shell commands bypass them. Fresh/resume/continuation CLI sessions and native inheritance were not certified. |
| Antigravity | `agy 1.3.4` | Rejected. Actual fake-Gemini fresh/resume/live-continuation probes enforce directory and exact-file file-tool grants. An approved Python shell script writes outside them. Native inheritance remains unverified. |

Codex pins `sandbox_workspace_write.writable_roots=[]`, clearing ambient roots
that otherwise merge with `--add-dir`. It pins `exclude_slash_tmp=true` and
`exclude_tmpdir_env_var=true`: **implicit temporary-directory grants are disabled
in this mode**. The workspace, explicit grants and separately allocated task
scratch remain writable. Requests without grants retain existing behavior.

Reads remain broader than these write scopes; no read privacy is promised.
Ordinary CLI children inherited the bounded paths, temp exclusions and blocked
network on fresh/resume probes, both fork modes, and hostile role configuration.
Guessed child-disable flags did not prevent spawning and are not used as a safety
claim. Polybridge's separately certified native adapter and workflow launches do
not accept grants. Native version pins remain unchanged. Other versions/platforms,
read-only and unrestricted configurations reject nonempty requests.

## Paths and reuse

- At most 16 existing absolute paths, each at most 4096 UTF-8 bytes and at most
  16384 bytes combined. Relative paths, `~`, control characters, missing/special
  paths, filesystem roots and canonical duplicates are errors.
- A directory covers descendants. An existing regular file covers its exact
  pathname, not its parent or siblings. Truncate, unlink/recreation and replacement
  from a writable source succeeded; sibling creation/rename was denied.
- Symlinks resolve to canonical targets. The requested path must continue resolving
  to the same target. Symlink escapes into unrelated directories were OS-denied.
- Requested/canonical paths, type, device/inode identity and certification are
  persisted and revalidated before launch, resume and fresh continuation. Missing,
  replaced or retargeted paths, changed versions and malformed authority reject
  reuse. **A legitimate atomic file replacement can also invalidate reuse.** Start
  a new explicitly authorized task for its new identity. A running harness grants
  pathnames, not immutable inode leases.
- Omitted nested requests inherit detected owner grants. Explicit requests may
  narrow to granted descendants/exact files or the owner's repository; they cannot
  widen scope. An empty request from a bounded owner rejects because reverting to
  legacy temp/config behavior could broaden access. Managed/nested rules still apply.
- Resume/continuation have no path override; they carry saved authority. Cancellation
  adds no authority. Incompatible recovery fails explicitly.
- Interactive takeover rejects bounded tasks because its command is uncertified
  for these grants. Use resume/continue tools; no unqualified interactive resume
  command is advertised for bounded tasks.

Caller detection and nested dispatch caps retain their existing best-effort limits;
they do not sandbox independent services or add restart-safe supervision.

## Reproduce

```sh
PB_CLI_INTEGRATION=1 uv run pytest -q tests/test_extra_path_cli.py
uv run pytest -q tests/test_extra_paths.py tests/test_extra_path_backends.py tests/test_extra_path_tasks.py
```

The opt-in CLI test uses an isolated localhost fake Responses API, makes no paid
calls and skips other versions/platforms. Support expansion requires new actual
allowed-write/denied-write evidence for each version and configuration.

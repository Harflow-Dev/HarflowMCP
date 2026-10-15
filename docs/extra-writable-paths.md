# Extra writable paths

`start_task(..., extra_writable_paths=["/absolute/build-output"])` authorizes
additional local writes without selecting unrestricted access. Inspect
`preview_task_permissions(repo_path=..., backend=..., extra_writable_paths=...)`
first: it returns requested/effective typed scopes, certification and enforcement
without launching an agent or allocating task scratch. Launch revalidates this
observation; preview does not reserve authority. For writable launches on scratch-aware
backends, enforcement includes `the task scratch directory (allocated per task at launch)`
as a symbolic future writable root. Read-only previews include no scratch root.

Installed-version qualification runs off the server event loop with a bounded
deadline. Bounded-mode execution uses the qualified absolute executable path;
runtime identity and path checks run again immediately before launch. The effective
repository, scratch directory and extra grants must not overlap the selected
launcher, its resolved executable or any intermediate symlink or lexical ancestor.
These checks precede the version probe and repeat at final validation. Every PATH entry must be absolute and nonempty: relative entries and empty segments (implicit cwd) reject extra-path requests before the version probe, since worker subprocesses run from the repository rather than the server cwd. Ordinary requests without extra grants retain existing behavior. Executables
with multiple hardlinks are rejected even when the requested grant is elsewhere:
this conservatively prevents writable indirect launcher aliases. The same effective authority also cannot overlap the server's
PATH directories (including absent higher-priority helper namespaces), selected helper/backend executable chains or targets, Python prefixes,
import search roots or server package code. Every selected helper and registered backend launcher must be native. Script launchers are rejected before version execution, even when a `pyvenv.cfg` or `package.json` container exists: wrappers can source or import dependencies outside that container. Ordinary requests without extra grants retain existing behavior. Container markers do not qualify arbitrary script dependency graphs. Recognized Python `bin/python*`
interpreter targets also protect their installation prefix, including a separate
base installation behind a virtualenv symlink. These restrictions prevent a worker
from poisoning unsandboxed repository probes, process checks, later version probes
or Python imports. They are recomputed before probing and at final validation.
Host execution also requires a bounded native Mach-O loader qualification. It
protects discovered absolute, `@loader_path`, `@executable_path` and `@rpath`
dependencies and lookup namespaces, including missing earlier candidates. Every
existing rpath candidate is qualified conservatively because dyld can skip an
existing image for architecture or compatibility; an unqualified candidate fails
closed. Native
interpreters discovered through absolute or `env` shebangs receive the same
dependency qualification; script interpreter layouts fail closed. Missing
system shared-cache names qualify only when an existing ancestor is independently
observed on a read-only filesystem. Malformed, unresolved or unknown loader
layouts, script host helpers and nonempty loader environment overrides reject
bounded requests without executing the unqualified helper. Script backends are
not assigned a generic dependency-graph certification. Ordinary script version discovery and native owner eligibility checks remain available; extra-path qualification rejects selected scripts before invoking version discovery.
Selected server helpers with multiple hardlinks are refused on mutable filesystems;
a filesystem reported read-only by `statvfs` is exempt from that alias refusal,
because a hardlink alias cannot cross filesystems. Direct overlap still rejects.
The Codex launcher itself retains its unconditional single-hardlink requirement.
Every effective writable repository, task scratch and directory grant is also
scanned, and every exact-file grant checked, for existing multiply-linked regular
files. All such files reject the request, even when their aliases are wholly
inside writable scopes. This conservative rule prevents an existing writable
hardlink from overwriting a source outside the sandbox boundary, including imported
server code. Actual isolated CLI probes demonstrated that existing aliases in
repository, directory and exact-file scopes can overwrite outside source files;
creating new cross-boundary hardlinks into a repository or grant was OS-denied
with both network settings. The check does not inspect arbitrary protected inode
graphs. Overlapping scopes are deduplicated, directory symlinks are not followed,
and in-scope targets are visited through their direct names. Child metadata and
directory opens use retained parent descriptors, preventing ancestor substitution
from redirecting traversal; observed directory paths are rechecked before acceptance. Inaccessible or
unstable entries, directory cycles, more than 100,000 metadata visits, depth over
128, or a traversal exceeding 10 seconds fail closed. The enclosing worker's
20-second budget also applies. These checks repeat at final validation; they
cannot freeze a concurrently mutated filesystem or interrupt a blocked syscall.
A server running from a source checkout therefore cannot grant writable authority
over its own source/import root; use an installed server for bounded tasks there.
On macOS, existing paths use kernel-derived spelling so case, Unicode and Data
firmlink aliases cannot hide protected namespaces. Symlink hops remain protected,
and missing names are anchored to their verified existing ancestors. Lookup
failures reject authority. The installed `/Applications/Polybridge Monitor.app`
bundle is protected as a whole, even when auto-open is off or the bundle is absent.
Auto-open names that bundle explicitly rather than using the URL-handler mapping.
Extra-path requests require `PB_OPEN_MONITOR` to be unset or different from `1`;
automatic app launch is outside this qualified runtime profile. Open Monitor
manually to follow these tasks. Ordinary requests retain optional auto-open.
Path resolution,
saved-authority reads, runtime qualification and final identity checks run in worker
threads with a 20-second budget per check, including executor queueing. There is
also one shared 25-second authority-validation deadline for each
start, resume, continuation or preview request, below the CLI's 30-second startup
handshake. A later background continuation receives a fresh deadline. Qualification
validates once, and execution rechecks after allocating scratch.
Timeout refuses the request with an actionable validation error; no agent is launched and
any pre-launch scratch is removed. A filesystem worker can finish after cancellation
or timeout, but performs only authority reads and cannot dispatch an agent. These
checks use a dedicated executor capped at four outstanding workers. Timed-out
workers retain their capacity until they actually finish; saturation refuses new
checks explicitly and leaves the server's shared executor available to unrelated
work. Python cannot interrupt a blocked filesystem syscall, so this budget does
not guarantee immediate process shutdown while such a worker remains blocked.
The CLI reports marked pre-launch path validation failures as `invalid_params`;
unmarked failures retain their existing launch-failure classification.

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
Only qualified configurations are enabled. Extra-path requests require notify, hooks, MCP servers and plugins to be absent or explicitly disabled, including referenced agent configuration files and profiles. Project `.codex/config.toml` files and symlinked project `.codex` directories are not qualified; direct grants inside the selected project's `.codex` are rejected. The pinned CLI preserves its native selected-project `.codex` write exclusion for missing configurations even with repository or ancestor directory grants. Global `CODEX_HOME/config.toml` and referenced role files (including their symlink paths and missing global configuration namespace) must stay outside all writable scopes. Configuration is bounded, parsed without execution, and revalidated before launch — including a closing snapshot recheck after the final launcher scans, which refuses configuration or runtime changes observed after those scans. Known auxiliary hook/managed configuration files, effective macOS managed preference inputs, and nonempty plugin state are outside the qualified profile and reject requests; missing auxiliary and plugin namespaces remain protected. Empty plugin directories are permitted. This is a conservative supported profile, not a generic proof of every plugin or configuration graph. Ordinary requests without extra paths retain existing behavior.

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

Bounded workers cannot write bridge task/control state: the default `~/.polybridge` state, and the active registry state (including custom task-directory aliases) are protected against repository or explicit grant overlap. This preserves saved authorization and lineage records. Only separately allocated per-task scratch remains writable; caller-supplied grants or repositories inside state/scratch are rejected. The shared registry check runs before qualification and again before execution, including permission previews. Ordinary requests without extra grants retain their existing behavior.

Extra-path qualification also protects known execution configuration namespaces of every registered backend, including backends absent from PATH. The backend declarations cover Claude's default/`CLAUDE_CONFIG_DIR` settings, MCP and plugin state; opencode's default and XDG config/data/cache/state paths plus `OPENCODE_CONFIG`, `OPENCODE_CONFIG_DIR`, `OPENCODE_TEST_HOME`, `OPENCODE_TUI_CONFIG`, `OPENCODE_TEST_MANAGED_CONFIG_DIR`, known system/managed preference inputs and inline configuration; Vibe's default/`VIBE_HOME` tools, plugins, agents and shared skills; and Antigravity's known `.gemini` and system settings namespaces. Existing other-backend configuration inputs are outside the qualified profile and reject requests, rather than claiming to parse arbitrary execution graphs. Relative or empty configuration-path overrides and nonempty inline configuration also reject requests. Missing namespaces, ancestors and symlink targets stay protected against repository, scratch and added-grant overlap. Use an isolated configuration environment; the feature does not modify user configuration.

Repository write authority is inherited from the existing `write_in_repo` contract. This feature adds no OS exclusions for other backends' latent project configuration files inside that already writable repository, and does not claim cross-backend repository configuration isolation. Explicit grants inside project configuration namespaces discovered from the current repository and its ancestors reject; a redundant repository grant preserves the same inherited authority. Existing project configuration inputs remain unqualified and reject requests. Those discovered namespaces retain protection when they lie outside the current repository. Ordinary requests without extra grants retain existing behavior.

Grants are filesystem write authority, not lifetime tracking of executable content.
An output directory may contain another checkout, a newly created nested checkout,
project hooks, or scripts that a later task could execute. Qualification does not
enumerate every possible future repository or prevent configuration creation throughout
an arbitrary directory grant. Selecting that directory as a later task's repository,
or executing its generated files, requires trusting those contents independently;
the original grant does not establish that trust. The runtime/configuration guards
protect the inputs selected for this dispatch and ambient backend configuration,
not every future consumer of authorized output.

The write boundary constrains the dispatched worker and certified children. Known
startup inputs must remain outside that worker's writable authority. Qualification
refuses changes it observes; it does not atomically pin configuration or executable
files. Independent processes, separately authorized tasks, or the OS user can mutate
those inputs between the last check and the CLI reading them. Protection against
non-cooperating concurrent writers requires a separately isolated or immutable
runtime and is outside this feature's certification. Keep the qualified startup
environment stable while launching bounded tasks.

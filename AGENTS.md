# HarflowMCP

Harflow engine: the stdio MCP server, `harflow-ctl` and `harflow-setup` CLIs, state persistence, and
agent and client integrations. It dispatches coding tasks to headless agents (Claude Code, Codex,
opencode, vibe, Antigravity `agy`) without blocking the caller. `README.md` is the user-facing
overview; the tool surface is the docstrings in `src/harflow_mcp/server.py`.

This repository is one of two in the Harflow workspace. The native macOS app lives in
`HarflowMac` and consumes this engine only through the public wire contracts below; it selects an
explicit engine version, so engine and app release independently. Never make a build, test or
release here depend on a sibling checkout.

Maintainer detail behind these rules is in [docs/architecture.md](docs/architecture.md); measured
CLI behavior is in [docs/backend-cli-facts.md](docs/backend-cli-facts.md).

## Documentation

- Keep `README.md` focused on the product: purpose, current capabilities and limitations,
  prerequisites, getting started, and links to maintained guides.
- Put version history, release notes, fixes, migration milestones, and other change records in
  `CHANGELOG.md`. Do not use the README as a changelog or implementation-progress log. Link to
  `CHANGELOG.md` from the README.
- Keep detailed current behavior and technical guidance in `docs/` (`docs/development.md`,
  `docs/contracts.md`, `docs/releasing.md` and the feature guides).

When a change affects user-facing behavior, setup, permissions, supported capabilities, commands,
or recovery, update `README.md` and the relevant guides in `docs/` as part of the same change.
Update affected tool docstrings and CLI help when their contracts change, verify documentation
examples and links, and remove resolved draft notes once their implemented behavior is covered by
maintained documentation.

Caller handoffs (prompts, follow-ups, live messages and workflow answers) are the caller's prose:
Harflow stores and delivers them verbatim, and the tool docstrings and caller guides say so,
telling callers to preserve spaces and word boundaries rather than concatenating words to save
tokens. The one deliberate whitespace collapse, `recent_activity`'s one-line summaries, is
display-only and documented as such.

## Commands

```bash
uv sync --locked
uv run pytest                                              # unit: fast, no auth, no tokens
HARFLOW_INTEGRATION=1 uv run pytest -m integration         # spawns real agent runs; costs real money
HARFLOW_CLI_INTEGRATION=1 uv run pytest -m cli_integration # real client and agent CLIs; no model calls in the measured cases, binary-dependent
uv run mcp dev src/harflow_mcp/server.py                   # MCP Inspector
scripts/check-private-refs.sh                              # private-reference guard (CI runs it too)
uv run python scripts/check-legacy-names.py                # legacy product-name guard (CI runs it too)
./install.sh                                               # install + register with the desktop app + each CLI found
uv tool install . --force --no-cache                       # reinstall after changes; --no-cache is required
```

CI (`.github/workflows/test.yml`) runs `uv sync --locked`, `uv run pytest` and both guards on an
Ubuntu/macOS x Python 3.11/3.13 matrix. Release validation and publication are described in
`docs/releasing.md`; pushes and releases need explicit authorization.

`uv tool install --force` alone reinstalls stale code: the version never changes, so uv reuses its
cached wheel and prints `Installed 1 package` while leaving the old files in place. Always pass
`--no-cache`, and verify by grepping the installed copy under
`~/.local/share/uv/tools/harflow-mcp/lib/python*/site-packages/`, not by trusting the output.

## Repository guards

- `scripts/check-private-refs.sh` keeps this open-source tree from naming a private project. Its
  patterns are assembled from pieces inside the script; never quote them elsewhere, and never write
  absolute paths under a personal home directory into tracked files.
- `scripts/check-legacy-names.py` rejects the pre-extraction product name and its environment
  prefix in every tracked or addable file, this one included. Surviving occurrences (legacy
  environment variables still honoured, stable wire fields whose rename is deferred, legacy state
  excluded from artifacts) are listed with exact counts and reasons in
  `scripts/legacy-name-allowlist.json`. Do not add an exception to avoid a rename; when one is
  justified, record the exact literal, count and reason.

## Qualification status

The CLI facts in [docs/backend-cli-facts.md](docs/backend-cli-facts.md) were measured on the
predecessor engine and do not certify HarflowMCP; `docs/development.md` is the maintained statement
of current limits. Requalify and update both rather than inferring support from a historical
measurement.

## Public wire contracts and state

The engine owns the public contracts: `harflow.ctl`, `harflow.setup`, `harflow.event` and
`harflow.version-info`, each wire `v: 1` inside a common envelope (`wire_contracts.py`), with
canonical fixtures in `contracts/v1` (`manifest.json` plus `manifest.sha256`). `docs/contracts.md` is
the specification: envelope validation, product-line compatibility, the version-info handshake,
event budgets, the selected state root and store ownership.

- A change to any document shape is a contract change. Unknown optional fields and unknown event
  kinds are additive; anything else needs a contract decision, updated fixtures and specification,
  and a coordinated update in `HarflowMac`, which must vendor byte-identical fixtures with the same
  manifest digest and gates each contract independently. Changing anything under `contracts/v1`
  means re-vendoring there; check the digests match rather than assuming. Identify the integration
  order before starting. A new event kind still means `events.EVENT_KINDS` here and the app's
  `TaskEvent.Kind` together; an older app decodes it as `.unknown`.
- A new ctl command that changes no existing document's shape is additive; the app treats an older
  engine's usage error for it as "unsupported". `status-batch --task-ids a,b,... --json` answers up
  to 100 ids in one document with per-id `errors` using the same `code` values as single `status`,
  and fails whole when caller authority is undecidable, never partially.
- The app acts only through `harflow-ctl` and `harflow-setup`; it never writes a record, phase file
  or inbox itself.
- State lives under the selected root (`HARFLOW_STATE_ROOT`, default `~/.harflow`), in
  `<state_root>/tasks`, owned through `.harflow-store.json` and `.harflow-store.lock`. Predecessor
  state is not imported and there is no migration command; never delete or edit the store marker or
  lock to get past a refusal.
- `ctl` reads share the server's checks through `workflow_node_reads` without importing `server`
  (or its MCP stack). The ctl paths that need registry-backed helpers (`permission-preview`,
  mutating workflow actions) install one with `server.install_cli_registry`. `run`/`resume` still
  build their own registry deliberately; see `retention.py` for why the sweep is a live-server job.

## Architecture rules

- **Nothing outside `backends/` may branch on a backend's name, and nothing outside `clients/` on a
  client's.** `if backend == "codex"` means the seam is missing a method. A new backend or client is
  one module plus a registry entry plus declared `Capabilities`; needing more means extending the
  seam. The pump in `tasks.py` reads capability fields, never names.
- **Unsupported requests fail loudly**, checked in both `server.py` and the backend (for example
  `max_turns` on Codex raises rather than being dropped), so callers are told, not surprised.
- **MCP status shaping lives in `server.py` (`_status_payload`), never in snapshots**:
  `Task.snapshot()`/`store.snapshot()` keep the full tail because `harflow-ctl --json` is a public
  wire contract. Reshape the MCP layer, not the snapshot.
- **Foreign client configs change only through each CLI's own `mcp add`**, never reformatted, because
  they hold application state and hand-commented config. The desktop config is merged, backed up
  and atomically replaced.
- **`inspect`/`remove` take the server key, not a `Registration`**, so an uninstall works after the
  binary is gone.
- **`Inspection.installed is None` means "could not tell", never "not installed".** Timeouts are
  `unknown`, not `failed`, because a timed-out CLI may already have written the config. Success is
  "add command succeeded", never "registered"; exiting zero is all that was observed.
- **Match CLI output signatures (for example "already exists" / "not found") whole, with the name
  delimited**, so a longer server name cannot match.
- `artifact_configuration_namespaces(repo)` is separate from execution qualification, and
  registration is bridge-owned bookkeeping, not a new execution or filesystem permission.

## Backend CLI rules

Before changing argv, flags, stdin wiring, stream parsing, classification or registration in
`backends/` or `clients/`, read [docs/backend-cli-facts.md](docs/backend-cli-facts.md). A behavior
change needs a re-measurement recorded there. The code rules embedded in those facts:

- Never symlink-resolve the `claude` path: it points into a versioned directory, so resolving it
  pins `PATH` to today's version.
- Validate `reasoning_effort` against Harflow's closed vocabulary, because CLIs degrade bad values
  silently.
- On claude, `publish` drops the git denies and adds `--allowedTools`, because the freedom's own
  allow entries would be defeated by its own denies (deny beats allow). `write_in_repo` with
  `allowed_commands` intentionally passes both: the unchanged denies stay and still win (measured), so
  no pattern can re-enable a direct `git commit` or `git push` command string there (a granted program may still invoke git itself).
- `assert_safe` validates the whole `Invocation` and refuses non-canonical tokens. Each backend's
  `assert_safe` defines the exact accepted shapes.
- A prompt never rides where a CLI can parse it as an option.
  - **Argv-carried prompts:** after `--` on claude (classic, capped shape), codex and opencode, or
    as one canonical `--prompt=` / `--print=` token on vibe and agy.
  - **Stdin shapes are supported transports, not exceptions to "fix":**
    - claude and agy live input uses `--input-format stream-json` with no positional prompt.
    - Assignments over 32 KiB use one-shot stdin, written once then closed immediately, on codex,
      opencode, vibe and capped claude runs. Uncapped claude stays on live stream-json stdin.
    - vibe's large prompts use an empty `--prompt=` and refuse prompts with outer whitespace.
  - **The scratch note travels only through a measured system-prompt seam**, declared as
  `Capabilities.system_guidance` and attached by that backend's `with_system_guidance`, never as a prefix to the
  caller's prompt (a leading `/slash` command must stay first, and handoffs are verbatim). Each `assert_safe` accepts
  exactly `system_guidance.TEXT`, once, in one position; vibe and agy declare none; a failure to attach it never blocks
  a launch.
- **One-shot stdin must reach EOF immediately**, because codex and vibe block on an open pipe.
    Live stdin closes only through the pump's close protocol.

## Invariants

Break these and the design stops holding. Full narrative in
[docs/architecture.md](docs/architecture.md).

1. **Only `_monitor` publishes a terminal status and sets `Task.done`**, after process exit and
   pipe drain. Cancellation sets `cancel_requested` and lets the monitor decide. The one status it
   must not publish is one for a process still alive: on its own `CancelledError` the server is
   being torn down, not the run, so it leaves the task `running` and re-raises, unless a
   cancellation is already in flight (`abandoned = not task.cancel_requested`), whose intent
   nothing on disk could reconstruct.
2. **Bookkeeping must never change an outcome.** A stale attribute in a log line once turned a
   successful run into `failed`; log calls and probes are individually guarded.
3. **Signal `Task.pgid` captured at spawn**, never `os.getpgid`, because pids get reused.
4. **Drainers are load-bearing** (an unread pipe blocks the agent), and waiting on them is bounded
   by `DRAIN_GRACE_SECONDS` (a grandchild can hold stdout open forever).
5. **Persist a session id the moment it is disclosed.** Codex reveals its `thread_id` mid-run;
   waiting until exit loses the chance to resume if the server dies first.
6. **Identity markers, not session ids, decide liveness** (`store.process_alive`,
   `store.record_process_alive`, `tasks._identity_markers`), because codex never receives its id on
   a fresh run and vibe renames its process after spawn.
7. **One live run per session**, checked against disk so two server processes cannot both resume it.
8. **`task_id` is validated before becoming a path**, because it arrives from a caller.

## Recovery and live input

Server restarts and recovered runs are explained in "When the client restarts the server" in
[docs/architecture.md](docs/architecture.md). Rules:

- Anything asking "has this settled?" goes through `resolve_status`, never `record.status`, or a
  wait returns early while claiming the full timeout elapsed.
- A terminal status with no exit code was never observed (`store.outcome_unobserved`). An
  unobserved `failed` is the only status the run's own output may overrule, because it is the one
  the backstop guesses; `cancelled` always stands. Reversing this turns a deliberate cancellation
  into `completed`.
- `classify` takes `exit_code: int | None`; never fabricate a clean exit. claude uses
  `exit_code is not None and exit_code != 0`, so a recovered success is not turned into a failure;
  codex and vibe require an observed zero exit.
- Do not remove the `abandoned` flag: without it, runs still alive were written off, and the
  session-busy guard let duplicates start in the same working trees.
- Every pump exit uses the one close protocol (flock the inbox, forward or report every queued
  message, seal, unlock, close stdin). Every send takes the same inbox lock, and nothing is awaited
  while it is held. `send` answers "queued", never "delivered".
- Queued follow-ups for tasks without live input (`followups.py`) are a separate ledger that never
  touches the inbox, its flock or the close protocol. Follow-ups are consumed only via the
  explicit `consume_followups` flag, never inferred from `task_id`; automatic continuations and
  workflow resumes never consume.
- Lock order: session lock (or the `continuation-<root>` chain lock), then `<id>.lock` and
  `<id>.followups.lock`; never the reverse. A deadlock-free order is the only thing keeping
  takeover, resume and retention from stalling each other.
- The resource ledger (`resources.py`) lock is a leaf: taken last, held for one bounded read-modify-write,
  never across an `await`, a subprocess (including `ps`), Git or another lock. Waiting for resources
  happens in the detached owner holding no lock; the launch receipt is the waiting task's only state
  (no TaskRecord until spawn), and uncertainty never frees an entry.
- Shared chain and group scratch (`shared_scratch.py`) is allocated by the launch owner under a per-folder `flock` that is a
  leaf lock (bounded poll before the pre-spawn window, held until the task record is durable, ordered after the session or
  chain lock and before `<id>.lock`); cleanup renames under it and deletes outside it, and refuses while anything that may use
  the folder is unsettled, uncertain, pending, pinned or holds an artifact there. Access states (`writable`,
  `no_additional_grant`, `sandbox_read_only`) report what was granted, never what a freedom name suggests.
- Task worktrees (`worktrees.py`) are created by the launch owner with `core.hooksPath=/dev/null`, never
  forced or removed automatically; a launch into one takes `<root>/.locks/<id>.lock` only to write its
  receipt, and `worktree-remove` is humans-only and fail-closed like takeover.

See "When the client restarts the server" in [docs/architecture.md](docs/architecture.md) for the
failure modes, accepted limits and the follow-up ledger sequence.

## Takeover and ctl

- Takeover is humans-only and ctl-only; never expose it over MCP. The interactive session runs under
  the user's own permissions, so an agent reaching it escapes its enforcement.
- Takeover gates use `lineage.detect_caller_detail`, never plain `detect_caller`, and fail closed:
  "could not look" is `caller_undecidable`, not "no caller".
- A takeover's `.ready` is terminal for lease purposes: its controller exits right after writing it,
  so `control.recover_abandoned` must never be pointed at the takeover family.
- The session is busy from `.req`, including pending attempts; `undecidable` counts as busy, and
  retention keeps a task exactly while it is busy.
- Never run interactive resume commands from tests: they open a TUI, and codex's writes a trust
  entry to `~/.codex/config.toml`.
- A forked `ctl run`/`resume` child redirects stdio first (a reader of the parent's stdout must see
  EOF) and clears the `identity.own_identity` cache. Test ctl forks from a fresh interpreter
  (`tests/ctl_driver.py`), because pytest's own process carries threads.
- ctl never starts retention or the notification worker, because the sweep is a live-server job.
  The ctl registry details are in [docs/architecture.md](docs/architecture.md).
- Automatic app opening is disabled (`tasks._maybe_open_monitor` returns immediately), but
  `HARFLOW_OPEN_MONITOR=1` still refuses extra writable path grants. The rules for re-enabling it
  are in [docs/architecture.md](docs/architecture.md).

## Enforcement must never overclaim

Every boolean in `Enforcement` is a strict claim: True only if the named thing genuinely cannot
happen. A caveat does not repair a boolean that says something untrue. For example,
`commit_push_blocked` is False for Claude because `git -C` and `bash -c` evade its deny patterns;
the weaker truth is `direct_commit_commands_denied: True`.

- `freedom` is a requested ordering, not four distinct strengths. Backends may collapse adjacent
  levels (opencode `publish` equals `write_in_repo`; vibe `publish` equals `unrestricted`), and
  every collapse must be listed, excluded from the cross-freedom refusal test and pinned by a test
  asserting the argv is identical.
- `publish_attempts_allowed_by_*` asserts only that Harflow removed the barriers under its own
  control; credentials, remote permissions, branch protection, hooks and the agent's own behaviour
  sit outside it. It is deliberately not `publishing_permitted`. Its suffix keeps the
  pre-extraction product name as a stable wire field, allowlisted in
  `scripts/legacy-name-allowlist.json`.
- `network_access` is separate, and `not_controlled` means the environment decides, which is not
  the same claim as `blocked`.
- When adding a backend or a freedom level, re-measure rather than reasoning, and record the result
  in [docs/backend-cli-facts.md](docs/backend-cli-facts.md).
  `tests/test_backends.py::test_enforcement_never_overclaims` enforces the shape; it cannot check
  whether the claim is true.

## Telling the caller

The caller is usually a model and only knows what the tool surface says. State changes it cannot
infer belong in the payload: `enforcement` on every task, `recovered: true` plus a `note` on tasks
from an earlier process, `next_step` when a wait returns still-running, `notices` for non-fatal
messages, and errors that say what to do instead of just what failed.

- A dispatch at `publish` or `unrestricted` reports whether the checkout is on the default branch,
  on `bridge_notices` (separate from `Accumulator.notices`, which vibe's `ingest` resets per turn).
  This is disclosure, not a guard: it cannot gate anything and never implies the run was held.
- The default branch is only what a remote's locally recorded `HEAD` says. Never fall back to
  guessing `main` or `master`, because a feature branch can carry either name. No fetch, no
  `ls-remote`, no `remote set-head`.
- Not being able to tell is reported too, and every probe failure is absorbed into that notice
  rather than escaping, per invariant 2.
- Wait defaults stay short because a client's request deadline is not discoverable. An explicit
  `timeout_seconds` up to 3600, or `until="terminal"`, is honoured as asked, never clamped. Waits
  are read-only and resumable, and never write, signal or resume (`wait_policy.py`).

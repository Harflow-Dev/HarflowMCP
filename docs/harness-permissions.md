# Harness permissions and workflow setup

> HarflowMCP extraction status: native workflow adapters and extra writable path grants are unavailable pending no-paid requalification. Historical certification details below describe the source implementation, not qualification of the renamed product. Ordinary headless dispatch is supported. Harflow macOS integration is unavailable pending companion runtime integration.

Before using a harness as a workflow node, configure its account, tools, and command permissions
for the work that node needs. HarflowMCP selects a permission mode; the harness still uses its own
configuration and credentials. A headless worker cannot answer an interactive approval prompt.

Standalone tasks can request [extra writable paths](extra-writable-paths.md) on
version-qualified configurations. File-tool permissions alone do not establish a shell write
boundary. Workflow and separately certified native-adapter launches do not accept these grants.

Audited **2026-10-06** against this checkout's backend adapters and current vendor documentation.
Mappings below describe HarflowMCP's launch behavior. Installed CLI versions can differ from current
vendor documentation; the Vibe example explicitly covers the locally inspected **2.25.8** version.

## First-time setup for every harness

1. Install the CLI and make its binary available on the `PATH` used by the HarflowMCP server or
   Monitor service: `claude`, `codex`, `opencode`, `vibe`, or `agy`. A working terminal alone does
   not establish that a desktop-launched process has the same environment.
2. Complete the harness's interactive login or provider/API-key setup before starting a workflow.
   Confirm that its configured model is accessible and has quota. HarflowMCP supplies no accounts.
3. Prepare the repository, dependencies, build tools, and any required external files. Use a known
   starting Git state so you can attribute changes and inspect the resulting diff.
4. Register required MCP servers **inside the worker harness**, configure their credentials, and
   permit the specific tools needed. For GitHub publishing, also prepare the Git remote and Git/CLI
   credentials and Git author configuration; `publish` does not authenticate Git, `gh`, Jira,
   or an MCP server. Model-provider login is separate from those accounts.
5. Check global settings, project overrides, agent profiles, and managed policies before granting
   commands. Merge additions into existing configuration; do not replace the entire file with an example.

`./install.sh` registers **HarflowMCP as an MCP server in supported clients**. That enables clients
to call HarflowMCP; it does not provision the services, accounts, or approvals used by workers.

In Monitor, Settings → General shows command-line tool resolution. The Location field remains a
draft until Return or **Apply** commits it, trimming surrounding whitespace. **Detect** clears the
saved override and refreshes resolution. Agents run with your login-shell PATH; **Environment
details** contains the full explanation, including exclusion of `HARFLOW_*` variables and
`HARFLOW_OPEN_MONITOR=0`.

### Before the first workflow run

- Check each node and fallback in its actual working directory: confirm effective global,
  project/local, profile and managed configuration. Multi-repository workflows may start above
  the individual checkouts, where their project rules are not loaded.
- Match the assignment's exact read/build/test commands, output directories and service tools
  to its saved access and network settings. Preserve existing deny/ask rules when adding allowances.
- Check binary resolution, dependency availability and credentials through the launching service's
  environment; for login-shell commands, confirm the shell's resolved PATH too.
- Verify a fresh small task with the same harness, version, directory, access and execution mode.
  Inspect structured denials and actual output before starting the full workflow. Model-backed
  checks can incur charges; use isolated no-model probes where available.

## Choose access for the node's work

| Node or control role | Default access | Setup needed for its assignment |
| --- | --- | --- |
| Planning | `read_only` | Repository reads/search; permit required read commands and read MCP tools. |
| Review | `read_only` | Diff/source reads; if review includes tests, allow their commands and necessary output paths. |
| Implementation | `write_in_repo` | Edits, exact build/test commands, writable outputs, and dependency access where required. `read_only` is invalid. |
| Task | `publish` | Match the actual action: use `read_only` for inspection or `write_in_repo` for local edits; retain `publish` when external mutation is needed. |
| Orchestrator | Pinned per-owner contract | Compatible same-harness native node candidates, including fallbacks and Current-mode descendants, determine the required access. The disclosed permissions also apply to the orchestrator itself. Child-mode owners receive separate contracts; historical runs retain their original permissions. |
| Workflow builder | `read_only` | Reasoning, repository inspection, and explicitly permitted context tools. |
| Start / End / Parallel boundaries | None | Structural nodes launch no worker and have no harness permission setup. |
| Run workflow | Child definition | Each child agent uses its saved access; nested invocation cannot raise it. |

Planning, Review, and Task can use all four access levels. Set the least scope that supports the
assignment, including test output and service calls. Saved node access is authoritative; a prompt
cannot raise it. Check fallback candidates too: switching harnesses changes the enforcement mechanism.

For example, a local code reviewer needs no publishing access. A Jira inspection needs an approved
read tool and credentials; a Jira update also needs publishing authorization and its write tool.
A read-only Codex reviewer cannot run tests that write into the checkout: adjust the node's access
or move verification to a suitable node.

## What each access level actually launches

| Harness | `read_only` | `write_in_repo` | `publish` | `unrestricted` |
| --- | --- | --- | --- | --- |
| Claude Code | `plan`; direct Git commit/push deny patterns | `acceptEdits`; same deny patterns (plus any `allowed_commands`, which cannot override them) | `acceptEdits`; Git commit/push allow rules only, with bridge deny patterns removed | `bypassPermissions`; bridge allow/deny patterns omitted |
| Codex | `read-only` OS sandbox | `workspace-write`; network off by default | `workspace-write`; network on by default | `danger-full-access` |
| OpenCode | `plan` agent | `build` agent | `build` agent; same mechanism as local writing | `build` plus `--auto` |
| Vibe | `ask` approval profile (not a write-tool exclusion) | `accept-edits` profile | `auto-approve`; same mechanism as unrestricted | `auto-approve` profile |
| Antigravity | `--mode plan` | `--mode accept-edits` | `--dangerously-skip-permissions`; same mechanism as unrestricted | `--dangerously-skip-permissions` |

These levels authorize an assignment; they do not make the five harnesses equally confined:

- **Claude:** restrictions use the harness permission layer, with no bridge OS sandbox. Accept-edits
  approves file edits, not arbitrary Bash or MCP calls. Publish adds only direct Git commit/push
  allowances; commands such as `gh pr create` still need their own approval rules. Deny rules win.
- **Codex:** HarflowMCP pins `approval_policy="never"`. Missing permission cannot be approved
  interactively. Workspace writes can include the checkout, temporary directories, configured
  writable roots, and the task scratch grant. There is no per-command commit deny list: a local
  commit can be mechanically possible at `write_in_repo`, even though publishing is not authorized.
- **OpenCode:** no bridge OS sandbox. The measured plan agent declined writes; that observation
  establishes model restraint, not guaranteed tool enforcement. Build already writes without
  `--auto`; auto-approval only affects requests that would otherwise ask and does not override denies.
- **Vibe:** no bridge OS sandbox. `ask` is an approval policy without a read-only write-tool exclusion;
  inherited user/project policies or custom profiles can pre-approve mutations, including Bash. Accept-edits approves edit tools but leaves Bash approval
  separate. Publish auto-approves tools broadly, rather than adding a narrow publishing exception.
- **Antigravity:** no bridge OS sandbox. Plan/accept-edits use the harness permission layer and
  settings. Builds/tests can be denied as mutating commands unless allowed. Publish bypasses the
  permission layer broadly, including operations outside the repository.

Write-capable workers receive `HARFLOW_SCRATCH` (alias `HARFLOW_TASK_SCRATCH`), and every worker receives
`HARFLOW_CHAIN_SCRATCH` and, in a group, `HARFLOW_GROUP_SCRATCH` (shared folders; see
[shared scratch](shared-scratch.md)). Claude and Codex also receive an explicit grant for each exact
directory they may write; `scratch.access` reports `writable`, `no_additional_grant` or `sandbox_read_only` per
path, so a shared folder exposed without a grant is never described as writable. The short note about these folders
travels in the backend's system prompt where a seam was measured, never in the caller's prompt. A scratch path does not
approve Bash commands or grant network access; other adapters do not gain confinement or a directory grant
merely because the path is provided.

## Network is a separate setting

| Harness/access | Omitted network setting | `network=true` | `network=false` |
| --- | --- | --- | --- |
| Codex `read_only` | Blocked | Refused | Blocked |
| Codex `write_in_repo` | Blocked | General sandbox network enabled | Blocked |
| Codex `publish` | General sandbox network enabled | Enabled | Blocked |
| Codex `unrestricted` | Not controlled by sandbox | Accepted | Refused |
| Claude, OpenCode, Vibe, Antigravity: all access levels | Not controlled by bridge | Accepted; no bridge barrier | Refused: backend cannot enforce a block |

Network enabled means general network access, not a Git-only exception or proof that a destination
is reachable. Blocked network can prevent dependency downloads and shell HTTP/Git requests.
MCP configuration and tool approval remain separate; network settings are not MCP authorization.
Codex network claims concern sandboxed worker commands, not model API connectivity or blanket
control of separately running MCP services. A blocked shell can coexist with model requests.
A run-wide `network=false` is incompatible with non-Codex nodes and fallback candidates.

## Per-harness preparation

### Claude Code

Complete Claude login/provider setup and inspect `~/.claude/settings.json` plus repository settings
such as `.claude/settings.json` and `.claude/settings.local.json`. `CLAUDE_CONFIG_DIR` can relocate
global settings. Read/review nodes may need read Bash permissions; implementation nodes need exact
test/build commands. A publishing node may additionally need `gh` or service-specific MCP permissions.

For a repository that uses `uv run pytest`, start with the exact rule
`"Bash(uv run pytest)"` in `permissions.allow`. If the assignment needs selected test arguments,
inspect that range before using `"Bash(uv run pytest *)"`. Merge additions with existing rules;
retain deny and ask rules. A matching deny or ask takes precedence over an allow, regardless of
specificity. Compound commands still need approval for their individual operations.
[Claude permission documentation](https://code.claude.com/docs/en/permissions).

### Dispatch-time command grants (`allowed_commands`)

Prefer `allowed_commands` over `unrestricted` when a task only needs to run specific commands, such as a test
runner or build, that `write_in_repo` would otherwise refuse with "This command requires approval". It is a bounded
list of Bash command patterns for one run, supported on Claude Code 2.1.310 (see `capabilities.allowed_commands` in
`list_backends`) at `write_in_repo` and `publish` only. Every other backend and access level refuses the field.

```sh
harflow-ctl run --repo . --prompt "Run the unit tests and fix failures" \
  --allowed-command "python3 -m pytest:*" --allowed-command "make lint"
```

Patterns become `Bash(<pattern>)` entries in the one comma-separated `--allowedTools` value, after the access level's
own entries. At `write_in_repo` the unchanged `--disallowedTools` stays next to them and still wins (measured), so the
deny patterns still refuse a direct `git commit` or `git push` command string. A granted program, or code it runs
(a Makefile target, a test script), can still invoke git; grants do not prevent commits.

**Limits.** At most 16 patterns, 256 bytes each and 2048 in total; no control characters, outer whitespace or
duplicates. `None` (omitted) grants nothing; on `resume_task` and `continue_task` it inherits the saved list, and an
explicit list replaces it after full revalidation (an empty list clears it). Automatic continuation inherits.

**Grammar.** Anything that does not match is refused, and the error names the pattern and the reason.

- Characters: letters, digits, space and `_ . / : = @ % + -`, plus one `*`. A comma is refused because it would split
  the `--allowedTools` value. Tokens are separated by single spaces.
- The first token is the program: a lowercase bare name, never a path, quote or environment assignment.
- `*` appears only as a separate final token (`make test *`) or as the `:*` suffix (`make test:*`), never inside a
  token (`pytest*` is refused). `:*` is the recommended form; `:*` and ` *` were measured to match the command with
  and without arguments, and neither matches a longer program name.
- A wildcard needs a literal non-option token after the program, such as a subcommand or target (`make test:*`,
  `yarn --cwd /repo tsc:*`). Option values are not parsed, so `arch -arm64:*` is refused by this rule and `nice -n 5:*`
  by the program list. An exact pattern needs no subcommand, except `make`, which always needs a target.
- Paths: a token containing `/` must be a plain relative path without `..` or an absolute path, and must resolve inside
  the repository after `realpath`; `--option=path` is accepted the same way. `-f/etc/x`, `key=/x` and `../x` are
  refused.
- `cp mv tee install touch mkdir truncate` and the destructive `rm rmdir dd chmod chown chflags shred unlink ln`
  accept only exact patterns, and every operand, bare names included, must resolve inside the repository.
- Refused programs: shells and evaluators (`bash sh env eval command`), `sudo ssh rsync`, wrappers (`nohup nice
  timeout time xargs arch caffeinate`), code evaluators (`awk sed perl ruby java swift osascript`), system tools
  (`kill launchctl defaults`), `docker podman kubectl nix`, and `git` at every access level.
- Interpreters (`python`, `python3`, `pythonX.Y`, `node`, `deno`, `bun`) are accepted only as `python3 -m <module>`
  or `<interpreter> <script with a script extension>`; `-c`, `-e` and similar forms are refused.
- Package launchers (`npx uvx bunx pnpx`, `npm exec`, `pnpm exec`, `yarn exec`, `uv run`, `poetry run`, `mise exec` and
  similar) validate the launched command recursively, at most two deep. Options before the launched command need a
  `--` separator (`mise exec -C /repo -- tuist install`); `--` is accepted nowhere else.
- `find` is refused with a wildcard or with `-exec`, `-delete` and similar actions.
- `gh` is refused below `publish`, where it is limited to `workflow run|view`, `run view|watch|list` and
  `pr create|view|list|checks|status`. This deviates from allowing `gh workflow run *` at `write_in_repo`.
- `git` is refused everywhere: `publish` already allows `git commit` and `git push`, read-only git needs no grant on
  2.1.310, and aliases make a subcommand list incomplete.

**What it is not.** The grammar is a guard against caller mistakes and obvious escalation, not a security boundary.
A grant is a prefix approval in Claude's permission layer, not OS enforcement; a granted program can run code from the
repository (Makefiles, test configuration, scripts); a wildcard lets it take any further arguments; a chained command
needs every part allowed; and user or project settings can add more allowed commands. Path containment is evaluated
at dispatch time and cannot prevent a later symlink change. The boundaries that do hold are the direct caller who
writes the grant and the nested rule below; the access level's deny patterns refuse only direct command strings.

**Nested dispatch.** A task dispatched from inside another Harflow task may grant only exact patterns the detected
caller holds, unless the caller is `unrestricted`. Omitting grants does not inherit them, and an undecidable caller
refuses the dispatch. Resume and continuation inheritance is subject to the same check.

**Version gating.** The installed Claude Code is re-qualified against the measured table before every launch, resume
and continuation. A run that carries grants is stopped, with a bridge notice naming both versions, when its init event
reports a different version or none, or a tool event arrives before init.

**Workflows.** Saved agent candidates (primary, fallbacks, on-demand nodes) carry their own `allowed_commands`; see
[workflows](workflows.md#candidate-command-grants). They are validated at save and launch for each candidate's backend
and freedom, an agent caller cannot save or start grants beyond its own, and a fallback never inherits the primary's.
`start_workflow` itself takes no `allowed_commands`.

**Disclosure.** `enforcement.extra_allowed_commands` lists the patterns, with caveats; the booleans are unchanged
(`direct_commit_commands_denied` stays true at `write_in_repo`). Task snapshots and `harflow-ctl status` also carry
`allowed_commands`, and `preview_task_permissions` / `harflow-ctl permission-preview` show the same without launching.

### Codex

Complete Codex account/provider setup and inspect `~/.codex/config.toml` (or `CODEX_HOME`), including
selected profiles, configured writable roots, and MCP server/tool policies. HarflowMCP explicitly
selects the sandbox and disables interactive approvals; changing an interactive approval preference
does not make a blocked headless command approvable.

Use `write_in_repo` for edits/tests and set the saved network option when dependencies require it.
The bridge uses `sandbox_workspace_write.network_access` explicitly where applicable. Check reported
writable roots before assuming repository-only writes. Prepare credentials and approved tools for
publishing separately. The bridge mappings are documented in the [Codex adapter](../src/harflow_mcp/backends/codex.py).
For per-command and per-patch caller approvals on macOS see [Codex caller approvals](#codex-caller-approvals-codex-app-server).

### OpenCode

Complete provider authentication and inspect global `~/.config/opencode/opencode.json` or
`opencode.jsonc` (`XDG_CONFIG_HOME` can relocate it), together with project and agent overrides.
Permit the specific shell/MCP operations required by the assignment; verify them with the installed
version's schema. HarflowMCP's `build` agent selection does not remove your configured denies.

Consult [OpenCode permissions](https://opencode.ai/docs/permissions/). The bridge's global MCP approval
editor supports the audited 1.x schema and refuses major version 2+ or an unverifiable CLI version;
it is not a universal configuration editor for every OpenCode release.

### Vibe

Prepare its provider credentials and model in `~/.vibe/config.toml` / provider environment, or the
directory selected by `VIBE_HOME`. Inspect project `.vibe/config.toml` and agent overrides as well:
HarflowMCP supplies `--trust`, so project configuration can affect a worker.

For **installed Vibe 2.25.8**, Bash uses literal command-prefix entries under `allowlist`. Merge
only the missing entries into the existing list and retain existing denies; this example illustrates
local Git inspection, not a replacement configuration:

```toml
[tools.bash]
permission = "ask"
allowlist = ["git status", "git diff", "git show"]
```

These prefixes also allow trailing arguments; they are not exact-command grants or glob patterns.
Use direct Git commands in the assigned working directory: HarflowMCP already sets it. Allowing
`git diff` does **not** allow `git -C /path/to/repo diff`. In the inspected 2.25.8 resolver, Git
option and repository checks recognize direct subcommands but miss the `-C` form. Adding a `git -C`
prefix can therefore approve options that direct Git would ask about, including `diff --output`
and `diff --ext-diff`. Use direct Git to retain those checks rather than adding `-C` prefixes.

For an assignment that explicitly needs a sibling repository, use a quoted directory change followed
by direct Git, for example `cd '/path/to/sibling-repo' && git diff HEAD`. Each command must be allowed:
merge the specific `cd '/path/to/sibling-repo'` prefix if needed, together with the required Git
prefixes. Installed 2.25.8 also requires an outside-directory grant: append this string to the
existing **Bash** `allowlist`, replacing the example with the repository path authorized for the
assignment, and retain `permission = "ask"`:

```text
"vibe-path:exact:/path/to/sibling-repo"
```

That typed grant covers the directory itself. Read relative source paths after changing directory,
for example `cd '/path/to/sibling-repo' && cat src/example.py`, or use a permitted Bash `grep`
command with a relative path. Keep Git commands free of global options such as `-C`. Absolute child
paths can require additional path grants; a legacy `/*` entry does not cover all descendants.

The directory grant does not grant a command, bypass Git guardrails, or establish read-only
confinement. Bash `cat`/`grep` reads do not inherit the native file tools' sensitive-file checks.
Add required test/build prefixes separately after inspecting their permitted arguments. Native
`read_file` and `grep` have separate path permissions and may still ask; do not copy broad recursive
grants into those tools to silence refusals, since their allowlist matches precede sensitive-file
checks in this version.

Current [Vibe permission documentation](https://docs.mistral.ai/vibe/code/safety-approvals-permissions)
and [configuration reference](https://docs.mistral.ai/vibe/code/cli/configuration-reference) call
the Bash key `allow`. Check the installed version before copying a schema; the example above is
version-specific. Outside-directory checks, project overrides, and profile rules can still deny work.
An `ask` request is auto-denied in headless mode. Raising access to Publish also selects the broad
auto-approve profile; it should reflect the assignment's intended scope, not just silence a refusal.

### Antigravity (`agy`)

Complete Antigravity authentication and inspect `~/.gemini/antigravity-cli/settings.json`, including
its command/MCP allow rules. Read commands can work in plan mode; accept-edits permits workspace
edits but builds/tests require command permissions. Add only the operations the assignment needs.
For instance, verify the project's test command is allowed before choosing agy for implementation.

See [command and tool permissions](https://antigravity.google/docs/permissions/),
[headless operation](https://antigravity.google/docs/cli/headless/), and the
[Antigravity adapter](../src/harflow_mcp/backends/antigravity.py) for measured permission caveats.
The bridge deliberately does not use agy's command sandbox, which did not establish usable,
consistent repository confinement in its recorded measurements.

## MCP approvals are separate from command permissions

Inspect a harness's global MCP approval entries without editing:

```bash
harflow-ctl mcp-allowlist --backend vibe --json
```

After registering the server and confirming its credentials, explicitly approve a needed tool:

```bash
harflow-ctl mcp-allowlist --backend vibe --allow jira/get_issue --json
```

Use your actual server/tool names. This command changes **global** harness configuration, backs up
existing content, and applies to future sessions; `--remove jira/get_issue` removes that approval.
It does not change Bash permissions, register the server, authenticate it, or enable network.
Project overrides and deny policies can still win. Claude/agy/Codex support `server/*`; Vibe requires
individual tools. Codex, Vibe, and OpenCode require an existing global server entry; OpenCode editing
also depends on its supported schema. See [approval adapters](../src/harflow_mcp/backends/mcp_approval.py).

## Candidate compatibility and verification

Vibe candidates must omit `model` and `reasoning_effort`: this adapter refuses overrides because it
cannot safely resolve their configuration precedence. Configure Vibe's model in the harness.
Turn caps are supported only by Claude and Vibe. OpenCode effort variants depend on the chosen
model; inspect `opencode models --verbose`. Antigravity accepts the bridge's low/medium/high effort
levels, but some model/effort combinations are refused by the CLI. Check all fallback candidates.
Native execution has additional version/model/access restrictions; use the
[native subagent support guide](workflows.md#native-subagent-execution) before assuming a preference will be honored.
Claude's certified read-only native child exposes only Read, Glob, and Grep. Choose Headless when
that worker needs shell Git, builds/tests, or MCP tools; a native preference may otherwise fall back
only for a declared unsupported combination, rather than anticipating the assignment's tool needs.

Before a full workflow, try a small node with the same harness, access, model, repository, and
execution mode. Model-backed smoke runs may consume tokens or incur charges. Start with a local
read task (inspect one file, and run `git status` when shell is available); for implementation,
use a disposable checkout and verify
one reversible edit and its test. Check the structured result, denial/warning evidence, and actual
outputs. Repeat for any service tool the real assignment requires, with its intended authorization.

## Troubleshooting a headless refusal

Capture the execution ID, harness version, requested access/network settings, working directory,
exact tool input, exit code, stderr, and structured denial/result before changing configuration.
Inspect the effective rules in the process that launches the worker; an interactive terminal can
have different settings and credentials. Avoid copying credential values into logs or reports.

| Symptom | Evidence and likely cause | Scoped remedy | Verification |
| --- | --- | --- | --- |
| A rule works in one checkout but not a workflow | Compare node working directory and loaded global/project/local/profile sources. A multi-repository parent may start outside each repository. | Put the minimum rule in the effective authorized scope, or start the assignment in its intended checkout. A file-access grant does not load another directory's project settings. | Fresh task in the same directory and execution mode; confirm the loaded setting sources. |
| Bash needs approval despite edit access | Compare the exact command, wrapper, arguments, deny/ask rules and compound operations. Accept-edits does not approve every shell tool. | Add an exact required build/test rule, preserving deny/ask entries. Do not approve every Bash operation or raise access simply to hide a refusal. | Verify both the intended command and an adjacent disallowed command in an isolated fixture. |
| Direct Git works but `git -C ...` fails | The command prefix or path differs. Vibe has additional version-specific checks described above. | Use direct Git in the assigned checkout; inspect a specific directory grant only if another checkout is authorized. | Run the actual read command in that directory and inspect denial evidence. |
| Plan, screenshot or scratch output cannot be written | Check target path, writable roots and protected configuration paths. A command allow rule does not grant filesystem access. | Use `HARFLOW_SCRATCH` (or `HARFLOW_CHAIN_SCRATCH` / `HARFLOW_GROUP_SCRATCH` for output a later run or sibling should find) for temporary assignment output, or an explicitly authorized output directory. Keep plans out of protected settings directories. | One reversible file creation in the intended directory under the same access, followed by cleanup. |
| Tool missing or wrong SDK/JDK selected | Compare service PATH, absolute binary, version and login-shell resolution. Startup files can replace environment values. | Select the required installed tool or provide the required environment through supported harness configuration. Avoid a blanket `env *` allowance. | Repeat the same command through the worker's shell and record its resolved path and version. |
| Dependency or network operation fails | Separate sandbox denial, DNS/connection failure, HTTP authorization and provider/API failure. | Prepare dependencies locally or enable the saved node's network setting when authorized and supported; repair destination credentials separately. | Check the intended endpoint without exposing secrets; then run the required dependency operation. |
| Login, provider or publishing authentication fails | Identify which process/service owns credentials and whether its provider/model is supported. | Complete that harness's login or service credential setup; retain repository scope and tool approvals. | Read-only authenticated status check in the same environment, then the authorized operation. |
| MCP call asks or is unavailable | Distinguish unregistered server, server authentication, tool allow policy and server failure. | Register/authenticate the server, then approve only its required tools. Bash/network settings do not approve MCP. | Read-only tool call in a fresh session, inspecting the server response and denial status. |
| Completed with warnings but output is absent | Inspect tool denials, actual changes and checks. Some harnesses can exit cleanly after refusal. | Fix the specific underlying cause; do not treat narration or completion status as proof of edits. | Verify the resulting files and final checks after any retry. |
| Run needs attention after a refusal or malformed result | Inspect the settled execution and the run's currently allowed controls. Settings changes do not settle ambiguous dispatches. | Use the explicit caller-authorized [control and recovery](workflows.md#control-and-recovery) action appropriate to the state. | Confirm settlement and the new execution result; do not duplicate an uncertain worker. |

For Claude, use `/status` and `/permissions` to identify effective sources and rules. Configuration
location and trust behavior are version-dependent; consult the current
[settings documentation](https://code.claude.com/docs/en/settings) rather than assuming a rule
saved in a sibling directory applies. Do not assume an environment assignment or `cd` wrapper
always prevents matching: current permission handling examines wrappers and compound commands.

On 7 October 2026, **Claude Code 2.1.292** was checked with an isolated localhost fake API and
`python3 --version`. Exact and wildcard Bash allows ran the command; a matching deny or ask
overrode the wildcard allow and was refused without interactive approval. These probes made no
paid model calls, changed no personal settings, and establish command-rule behavior, not that
every build command or project configuration is ready.

### macOS Git shim warning under read-only Codex

An xcrun cache diagnostic on stderr can coexist with a successful Git command. On 7 October
2026, **Codex CLI 0.160.1**, **macOS 26.6.2**, and **Xcode 26.6.0** were measured with the
explicit `:read-only` sandbox profile and no model requests:

| Probe | Exit | Observed stderr / resolution |
| --- | --- | --- |
| `/usr/bin/git --version` | 0 | `couldn't create cache file '/tmp/xcrun_db-...' (errno=Operation not permitted)`, plus confstr/cache and Xcode event-stream diagnostics. |
| Selected Xcode's `usr/bin/git --version` | 0 | Empty stderr; same `git version 2.50.1 (Apple Git-155)`. |
| `/bin/zsh -lc 'command -v git; git --version'` with existing startup configuration | 0 | Resolved selected Xcode Git; empty stderr. |
| Same login shell with an empty isolated `ZDOTDIR` | 0 | Resolved `/usr/bin/git`; reproduced cache diagnostics. |

The shim attempts cache writes denied by this read-only profile. Direct selected-Xcode Git is
an observed workaround for the measured command. Discover its path with `xcode-select -p`
outside the sandbox, then invoke that executable directly under the same sandbox. Verify the
actual intended Git operation too; this measurement does not certify every Git subcommand.
Do not broaden filesystem writes just to suppress this warning. Login-shell PATH can override
the launch environment, so verify `command -v git` in the actual shell before changing PATH.

Reproduce without a model call or personal configuration changes:

```bash
python3 scripts/probe-macos-git-sandbox.py
```

The script uses temporary Codex configuration and repository directories, bounds each process,
and prints versions, exit codes and captured diagnostics as JSON. Run it outside an existing
sandbox: nested sandbox installation may itself be refused. In this installed CLI,
`codex sandbox -P :read-only -- ...` requires a permission profile; there is no `macos` subcommand.

Warning reproduction with exit 0 does not prove worker failure or explain a refusal. Collect the
real execution's structured tool/result record before attributing a failed run to xcrun, and
preserve the run's recovery and caller-authority requirements.

Changing harness settings does not raise a saved node's access or automatically make an old refused
execution retryable. Follow [workflow control and recovery](workflows.md#control-and-recovery) for
the current run state; a fresh task or workflow can verify the corrected setup.

Bridge mapping sources: [Claude](../src/harflow_mcp/backends/claude.py), [Codex](../src/harflow_mcp/backends/codex.py),
[OpenCode](../src/harflow_mcp/backends/opencode.py),
[Vibe](../src/harflow_mcp/backends/vibe.py), [Antigravity](../src/harflow_mcp/backends/antigravity.py).

## Exact-action caller approvals

`caller_approvals` is a strict boolean, defaulting to `false`. The adapter is limited to
non-native uncapped Claude Code stream JSON on a measured version, or Codex through `codex app-server` (see
[Codex caller approvals](#codex-caller-approvals-codex-app-server)), at `write_in_repo` or `publish`, with
foreground-owned permission requests. Other backends, native children, explicit turn caps,
read-only and unrestricted launches reject the opt-in before spawning. The capability catalog
reports `supports_caller_approvals` and `caller_approval_support` (`versions` as a list, `tools_by_version`
and `path_safety_prompt_versions`); the installed version is qualified again at every launch, resume
and continuation, and a persisted version is never trusted for authority. The Claude qualification
table (`backends/claude_qualification.py`) is the single source:

| Claude Code | Caller-approvable tools |
| --- | --- |
| 2.1.302 | Bash only; `caller_write_approvals` is refused |
| 2.1.310 | Bash, Read (including path-safety and out-of-repository Reads), and Write and Edit with `caller_write_approvals` |

No alternative transport or permission mode is selected implicitly. If Claude's `init` event reports a
version other than the one qualified before launch, or a permission request arrives before any version
was reported, the run is stopped, pending approvals are invalidated and a bridge notice names both
versions.

**Opt-ins.** `caller_approvals` covers Bash and, on 2.1.310, Read. Approving a Read can disclose files
outside the repository. `caller_write_approvals` (strict boolean, default `false`, requires
`caller_approvals`, available on `start_task`, `preview_task_permissions`, `harflow-ctl run` and
`permission-preview` as `--caller-write-approvals`, and as a saved workflow candidate field) additionally
lets a caller approve individual Write and Edit requests, including outside the repository. Resume and
continuation inherit it and cannot override it. A nested task may enable it only if its detected caller
has it, and the nested enforcement comparison treats `enforcement.caller_approval_tools` as narrowing-only.
Without the opt-in a Write or Edit request is answered with a delivered refusal (see below) and the run
continues.

**What a Write or Edit approval means.** Each approval covers one exact action: Harflow echoes the exact
input and applies no session grant, directory grant or `permission_suggestions`. A second write to the
same directory asks again. Approving a write outside the repository widens `write_in_repo` for that one
action, so `enforcement` lists `caller_approval_tools` and a caveat when it is on, and the task snapshot
and `harflow-ctl status` carry `caller_approved_writes`: request id, tool, `file_path`,
`inside_repository`, `decision`, `effective_decision`, `delivered_decision`, `expiry_rejection`,
`delivery`, `execution_outcome` and `write_possible`. That list never contains file content or edit
strings; those stay in the private ledger and in `get_approval_request`.

**Strict input schemas.** Read accepts an absolute, NUL-free `file_path` plus optional integer `offset` and `limit` and string
`pages`. Write accepts exactly `file_path` and `content`, Edit exactly `file_path`, `old_string`,
`new_string` and optional boolean `replace_all`; Write and Edit paths must be absolute. Unknown keys stop
the run as an unsupported request. Ledger rows keep Claude's `decision_reason` (at most 1 KiB) and
`decision_reason_type`.

**Harflow system rejections.** Some requests are answered by Harflow itself, never offered to the caller:
the row is admitted with `decided_by: "harflow"`, an effective rejection and delivery reserved (rejections decided at admission; an expired request keeps the caller's recorded `decision` with `expiry_rejection: true`, and the delivery-time path conversion below keeps `decision` and `decided_by: "caller"` with `system_rejection` set; either refusal is still Harflow's), and the
existing pump sends `Harflow refused this action: <reason>`. Reasons are `write_approvals_not_enabled`,
`protected_path`, `request_too_large`, `approval_expired` (an expired request, including a caller approval that expired before delivery; the caller's `decision` stays recorded with `expiry_rejection: true`) and `path_changed_since_admission` (the delivery-time path re-check below; the caller's `decision` stays `approve` and `decided_by` stays `caller`, while `system_rejection` carries the reason, `effective_decision` is `reject` and the row ends `system_rejected`). Apart from the delivery-time conversion, these are not caller decisions: callers cannot answer them,
`encode` never produces an approval for them, and task classification reports them as
`harflow_policy_rejected`, not `caller_rejected`. An over-budget Read, Write or Edit request whose ids are
still valid is kept as a bounded audit row (ids, tool, `file_path` under 4 KiB, size and the SHA-256 of
the original action, no content). A full or invalid ledger keeps the stop-the-run behavior.

**Protected paths.** Write and Edit approvals are refused (`protected_path`) for the Harflow state root,
repository and user Claude settings and policy (the whole `<repo>/.claude/` directory, including hooks,
agents, commands and settings, `CLAUDE_CONFIG_DIR` or `~/.claude`, managed settings, `~/.claude.json`, `.mcp.json`) and the effective Git
hooks and `config` locations (`git rev-parse --git-path hooks`, the common directory and the worktree
gitdir) plus the `hooks/`, `config` and `config.worktree` of every Git directory above the target: one named `.git`, or holding `HEAD` with `objects/` and `refs/` or a `commondir` (bare repositories, submodule and linked-worktree gitdirs). Read is refused only for the state
root. Targets are resolved through symlinks (or their nearest existing parent) and compared
case-insensitively; if the Git locations cannot be resolved, Write and Edit are refused. These checks are re-resolved at every Write and Edit admission (nothing is cached on the task, so a mid-run
`core.hooksPath` change is honored, and a resolution failure refuses). A Bash approval is checked only as the exact
command shown to the caller. The 64 KiB request limit covers the whole control-request JSON.

**Delivery-time path re-check.** Admission records, per row, a private `path_snapshot` (each path as the backend named it and as resolved through symlinks; it is not
exposed by `get_approval_request` or `list_approval_requests`). Immediately before the pump writes an approve for an action that names paths (Read, Write, Edit, Codex file changes), it
re-resolves every path through the backend, compares them with the snapshot and re-runs the protected-target checks against the targets as they are now (off the event loop). A
changed resolution, a changed path set, or a missing or malformed snapshot is refused as `path_changed_since_admission`; a path that became protected
without moving (for example a changed `core.hooksPath`), or that cannot be resolved, is refused as `protected_path`. The refusal is a Harflow system rejection recorded in its own step before the send reservation, so a crash between the two can never leave an approve to write,
and nothing is resent after `sending`. A request that fits the request budget only without its snapshot is refused at admission as `request_too_large`, kept as a bounded audit row (ids, tool, size and the SHA-256 of the action, no input), and the run continues; there is no separate snapshot cap. A failure to resolve the paths, at admission or at delivery, is refused as `protected_path`. Rejections the caller chose are delivered unchanged, and commands (no paths) are unaffected. Residual windows remain: the awaits between the re-check and the write,
and the agent's own write after the approve is delivered. A hard link created to a protected file is not detected, because path resolution cannot see it. Admission does not
refuse symlinked ancestors inside the workspace, since legitimate repositories use symlinked directories; the delivery re-check covers a swap instead.

The adapter retains `acceptEdits`, saved Git deny/allow rules and the harness's managed policies.
It cannot override an explicit deny, plan mode or a sandbox restriction. Bash requests that ask to
disable the harness sandbox are unsupported. Claude does not gain a bridge OS sandbox through this
feature. Some actions are already allowed or denied by the harness and never ask the caller.
Agent prose, error messages and completed denials never create actionable approval requests.

Standalone MCP example:

```text
start_task(prompt="Run the requested check", repo_path="/path/to/repo",
           backend="claude", caller_approvals=true)
list_approval_requests(task_id="TASK_ID")
get_approval_request(task_id="TASK_ID", request_id="REQUEST_ID")
respond_approval_request(task_id="TASK_ID", request_id="REQUEST_ID",
                        action_digest="DIGEST_FROM_GET", owner_revision=1,
                        decision="approve")
```

Inspect the complete action returned by `get_approval_request` before choosing `approve` or
`reject`. Approval sends the exact original input once. Suggestions to change permissions,
settings or modes are never applied. Decisions are separate from ordinary messages and do not
consume user turns. A reserved decision, a local stdin write (`sent`) and an observed tool result
(`execution_outcome`) are different facts; `sent` is not a harness acknowledgement or proof of
execution. Duplicate, stale, mismatched and expired answers fail rather than being replayed.

The equivalent CLI commands are:

```bash
harflow-ctl run --backend claude --repo /path/to/repo --prompt 'Run the check' --caller-approvals [--caller-write-approvals] --json
harflow-ctl approval-list --task-id TASK_ID --json
harflow-ctl approval-get TASK_ID REQUEST_ID --json
harflow-ctl approval-respond TASK_ID REQUEST_ID --action-digest DIGEST --owner-revision REVISION --decision reject --json
```

Workers, orchestrators and their children cannot approve their own work. Standalone requests
default to caller ownership. Monitor launches explicitly select Monitor ownership through the
existing verified-human path; `--monitor` alone cannot bypass caller/ancestry checks. Workflow
requests inherit their authoritative root owner. On an engine advertising
`caller-approvals.monitor-respond.v1`, the Monitor ctl channel may answer caller-owned requests as
the local human; Monitor-owned requests remain Monitor-only, and a caller cannot answer a
Monitor-owned request. The response channel is derived from ctl authorization, not supplied as a
free role. `decided_by` continues to mean caller versus Harflow; optional `responder` records
`caller` or `monitor`. This uses inferred, cooperative authority under the existing same-OS-user
metadata boundary. It does not authenticate the Mac app or human and does not prevent a malicious
same-user process from evading inference. Authenticated Monitor identity is tracked separately in
[issue #43](https://github.com/Harflow-Dev/HarflowMCP/issues/43), outside this capability.

Requests expire 1800 seconds after receipt, or at an earlier workflow node execution deadline.
Pending requests keep stdin open and pause bridge inactivity expiry, while hard deadlines and
cancellation remain effective. Caller disconnect does not cancel the request. Expiry sends a
reject only to the same verified live process/request; lost ownership leaves uncertainty and no
blind resend. Restart does not reattach a lost stdin owner. Cancellation and process exit invalidate
unanswered requests. Settle and inspect the prior execution before explicit recovery; changing
configuration or resuming is a separate action and never a permission response.

**Approval timeout.** The request deadline is the earlier of 1800 seconds after receipt and the workflow
attempt deadline. Expiry means Harflow tries to deliver a rejection (`Harflow refused this action:
approval_expired`) to the verified live process; it never approves, even when the caller's `approve` was
reserved but not yet written, and the caller's `decision` stays recorded. Delivery (`sent` or
`delivery_unknown`) and the observed outcome (`execution_outcome`) are separate facts, and a failed delivery
is never resent. Answers after the deadline, or with a stale revision, are refused. Expiry does not time
a node out: a workflow node times out only through its existing attempt timeout.

**Recovery after a denial.** Recovery is always a new `resume_task` or `continue_task` execution of a finished
task, never an approval, and what to change follows `denial_remedy`. `allowed_commands` covers only Bash
`approval_required` and `chained_command` parts. A denied or expired Read, Write or Edit approval needs a
resumed run (with `caller_approvals` and `caller_write_approvals` inherited) so the agent can raise a fresh
request. `path_safety` and `outside_working_directory` go through caller approvals, not grants, and a
`deny_rule` cannot be lifted by grants. See
[task continuation](task-continuation.md#recovery-after-a-permission-denial). A request that was denied or
expired cannot be approved afterwards.

**Current limits.** Not available: retrying a permission-blocked workflow node with a narrow grant
(a caller-authorized `permission_grant` retry needs its own state machine), approval notifications,
`allowed_commands` on non-Claude backends, approvals outside qualified Claude and Codex app-server transports, and approvals for native workflow children.

Storage is bounded per task: 64 KiB canonical request payload, 16 pending requests, 128 lifetime
records and a 2 MiB ledger, with 256 KiB reserved for decisions/outcomes. Full or invalid ledgers
stop owned execution without approving. Ledgers require a compatible Harflow header and matching
task identity on every request; unqualified or unreadable state reports approval uncertainty and
cannot authorize a decision. Preserve old state and select a separate fresh missing or empty
compatible root for recovery; no conversion or repair is provided. See
[approval ledger contracts](contracts.md#task-approval-ledgers). Records remain until normal task
retention/deletion.
Lists contain metadata only (16 by default, maximum 32). Active requests appear first, unresolved
delivery uncertainty next, and retained history last; each group is oldest first, with request IDs
breaking timestamp ties. This order is applied before pagination. Full commands may contain sensitive
arguments and appear only in authorized full reads and the protected local ledger.

Root-tree reads have shared source byte/count budgets and return unavailable/retry guidance rather than
silently omitting unverified pending work.

Harflow native app integration and automatic opening are currently unavailable.
Setting `HARFLOW_OPEN_MONITOR=1` does not enable opening until the companion
runtime is integrated; the setting remains reserved for that integration.

### Codex caller approvals (`codex app-server`)

`caller_approvals` on Codex runs that one task through `codex app-server` instead of `codex exec`; without the opt-in the exec path is
unchanged. It is qualified for codex-cli 0.163.0 on macOS only (`backends/codex_qualification.py`), at `write_in_repo` and `publish`,
for non-native uncapped tasks, and not for saved workflow candidates. Any other version, platform or freedom refuses at dispatch.
`caller_write_approvals` additionally lets a caller approve file changes.

**What an approval means.** Each approval covers one exact command shown to the caller. Accepting it may run the command outside the
OS sandbox (Codex does not say which approvals leave it), and a command approval is not path-checked: an approved command can modify the
repository's Git config and hooks, `CODEX_HOME` and Harflow's own state, so `enforcement` and `preview_task_permissions` say so. Harflow
sends only `accept` (caller approval) and `decline` (caller rejection, Harflow rejection, expiry); it never sends `acceptForSession`
or an exec-policy amendment, which would persist a grant. The request shows `kind`, `command`, `cwd`, `reason` and the proposed amendment (never acted on).

**File changes.** With `caller_write_approvals`, an `item/fileChange/requestApproval` is joined with the preceding `item/started` item
of the same thread and turn; the paths, kinds and diff are frozen into the digest (at most 32 KiB). Every source and destination path,
including a rename's destination, is checked against the protected targets: the Harflow state root, Git hooks and config, the resolved
`CODEX_HOME`, and `.codex` and `.agents` in every directory from the Git root to the working directory and in each linked-worktree root. The Codex sandbox itself protects `.git`, `.codex` and `.agents` at the Git root and the working directory (measured on 0.163.0); the intermediate directories and linked roots are a conservative extension. A request with `grantRoot`, without a started item, with an
ambiguous one, with relative paths or over the size bound is declined by Harflow. If the completed item's changes differ from the approved
ones (compared as complete change objects) the row records `execution_outcome: "mismatch"` and the run stops. A multi-path action reports `file_paths` with a containment fact per path.
Paths are checked at admission after symlink resolution and again, with the same checks, just before an approval is written (see the delivery-time path re-check above); a swap after that point is not prevented. Without the opt-in these requests are declined.

**Pins and verification.** The server starts with `approval_policy="on-request"`, `approvals_reviewer="user"`, the freedom's sandbox mode,
`sandbox_workspace_write.writable_roots=[]`, the resolved network setting, `features.hooks=false`,
`features.request_permissions_tool=false`, `features.exec_permission_approvals=false` and `shell_environment_policy.inherit="core"` on `-c`, in
the `thread/start` or `thread/resume` `config` map and as explicit parameters. Before the turn starts Harflow checks the reported version,
policy, reviewer, sandbox type, empty writable roots, network, workspace roots and that `hooks/list` is empty; any mismatch stops the task. `turn/start`
carries only the thread id and the prompt. A hook, auto-review or guardian notification stops the run and is recorded as an audit entry.

**Repository configuration is ignored.** Project trust is overridden to `untrusted` in the config map for the working directory, every
ancestor up to `/` and the Git root, so the repository's own `.codex` config, rules, hooks and MCP servers do not apply, and nothing is written to
`CODEX_HOME/config.toml`. The Git root is resolved with repository configuration neutralised and must be the nearest ancestor holding `.git`;
a repository without one is refused. For a linked worktree the main checkout and the common directory (and their ancestors) are overridden and scanned too, and the worktree's `.git` file must verify as
`<common>/worktrees/<name>` with Git's own back-link; a submodule-style or unverifiable `.git` file is refused. As a backstop, every `.codex/rules` from the Git root down to the working directory must also pass the rules check below.

**Dispatch refusals (every start and resume).** The task is refused when `CODEX_HOME` lies inside a sandbox-writable location (the workspace,
`$TMPDIR` or `/tmp`); when `CODEX_HOME/config.toml` sets `profile` or cannot be parsed; or when a rules file Codex loads (`*.rules` directly in
`CODEX_HOME/rules` or a project `.codex/rules`) is not only `prefix_rule(pattern=[...], decision="prompt"|"forbidden")` calls, because an allow rule (including
the default decision, which is allow) runs commands without approval and outside the sandbox. To make a refused `CODEX_HOME` work, change those
rules to `prompt` or `forbidden`, remove them, or use a separate `CODEX_HOME` for Harflow tasks. User MCP servers and
`shell_environment_policy.set` entries stay in effect and are listed in `enforcement.caveats`; MCP tool calls that need approval are always rejected, and so are
permission, user-input and every unmeasured server request.

**Limits.** One approval per `itemId`: a second request for the same item (for example `write_stdin` to a running approved command) stops the task,
keeps the first row's state and attributes no completion to it. A request after the turn ended, outside the active turn, or without `accept` and `cancel`
decisions stops or is declined as above. Process identity uses the process start time at one-second resolution plus the binary and `app-server` markers.
Stop order: after the turn completes stdin is closed and the server gets a bounded time to exit; a server that does not is sent SIGTERM to its process group, then swept
with its tool process groups (their identities are recorded beforehand and a group is only signalled while a member still matches its recorded start time) with the observed
turn outcome preserved. A cancel signals the group with SIGTERM at once instead of waiting for EOF. Remaining risk: tool groups of a server that dies or is killed before
terminal evidence, or when Harflow itself is killed, can be left running, as with the exec path.

**Denials.** `permission_denials` entries are classified from Harflow's own evidence: `caller_rejected` and `harflow_policy_rejected` from the answer written for that
exact ledger identity, `policy_denied` from the exact stderr router line for an exec-policy `forbidden` rule while a turn is active, and, on the exec path only,
`approval_policy_never` from its stderr router line. Everything else, including sandbox denials, is `unclassified` (see [task outcomes](task-outcomes.md#permission-denial-classification)).


## Capability discovery and publication disclosures

Call `list_backends` before selecting optional permissions. Each entry reports
`capabilities.extra_writable_paths` (bounded support, certification and refusal reason),
`capabilities.allowed_commands`, and `capabilities.caller_approval_support`. Certification is
specific to the installed harness and configuration. Unsupported bounded requests fail before
launch. At `unrestricted`, extra paths are validated and disclosed as a no-op; they do not narrow
access or establish bounded-write certification.

`enforcement.publish_attempts_allowed` describes Harflow's authorization to attempt publication.
The deprecated legacy alias remains identical for this release. Neither key guarantees publication
or implies that network access works. Default-branch notices describe the checkout observed at
dispatch; they do not claim where a later commit or push occurred.

## Read-only plan files and harness state

Read-only restricts agent tools according to each backend; it does not mean the CLI process leaves
no state. Harness-owned session history, logs and configuration can persist in user state
directories even when tool writes are sandboxed. Claude plan mode additionally permits its built-in
plan file under `~/.claude/plans`, `CLAUDE_CONFIG_DIR/plans`, or a project-contained
`plansDirectory` from settings. Enforcement explicitly discloses this exception. Vibe's `ask` profile does not impose a read-only write-tool exclusion; its shell is not an OS boundary.
Other agent-level modes must not be read as universal filesystem-write guarantees.

Claude 2.1.310 accepts custom plan directories only within the project root, so this implementation
does not redirect plans into external task scratch or add a writable-directory grant. For read-only
runs, Harflow joins top-level Write/Edit calls to successful tool results and records existing
regular files directly in a recognized plan directory as metadata-only artifacts through a separate
bounded worker. Saved directory identity makes later availability independent of reader settings. No target content
is read, no arbitrary user config file becomes registrable, and symlinked files/ancestors are
rejected. Built-in artifact provenance is an observation of the tool result, not verified contents
or exclusive authorship. Unreported tools, subagent-only writes, disappeared files, replaced config
and failed bookkeeping can leave tracking incomplete; a notice reports observation failures. The
permission caveat still applies when artifact capture is unavailable.

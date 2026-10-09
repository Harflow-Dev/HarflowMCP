# Harness permissions and workflow setup

Before using a harness as a workflow node, configure its account, tools, and command permissions
for the work that node needs. Polybridge selects a permission mode; the harness still uses its own
configuration and credentials. A headless worker cannot answer an interactive approval prompt.

Audited **2026-10-06** against this checkout's backend adapters and current vendor documentation.
Mappings below describe Polybridge's launch behavior. Installed CLI versions can differ from current
vendor documentation; the Vibe example explicitly covers the locally inspected **2.25.8** version.

## First-time setup for every harness

1. Install the CLI and make its binary available on the `PATH` used by the Polybridge server or
   Monitor service: `claude`, `codex`, `opencode`, `vibe`, or `agy`. A working terminal alone does
   not establish that a desktop-launched process has the same environment.
2. Complete the harness's interactive login or provider/API-key setup before starting a workflow.
   Confirm that its configured model is accessible and has quota. Polybridge supplies no accounts.
3. Prepare the repository, dependencies, build tools, and any required external files. Use a known
   starting Git state so you can attribute changes and inspect the resulting diff.
4. Register required MCP servers **inside the worker harness**, configure their credentials, and
   permit the specific tools needed. For GitHub publishing, also prepare the Git remote and Git/CLI
   credentials and Git author configuration; `publish` does not authenticate Git, `gh`, Jira,
   or an MCP server. Model-provider login is separate from those accounts.
5. Check global settings, project overrides, agent profiles, and managed policies before granting
   commands. Merge additions into existing configuration; do not replace the entire file with an example.

`./install.sh` registers **Polybridge as an MCP server in supported clients**. That enables clients
to call Polybridge; it does not provision the services, accounts, or approvals used by workers.

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
| Claude Code | `plan`; direct Git commit/push deny patterns | `acceptEdits`; same deny patterns | `acceptEdits`; Git commit/push allow rules only, with bridge deny patterns removed | `bypassPermissions`; bridge allow/deny patterns omitted |
| Codex | `read-only` OS sandbox | `workspace-write`; network off by default | `workspace-write`; network on by default | `danger-full-access` |
| OpenCode | `plan` agent | `build` agent | `build` agent; same mechanism as local writing | `build` plus `--auto` |
| Vibe | `plan` profile | `accept-edits` profile | `auto-approve`; same mechanism as unrestricted | `auto-approve` profile |
| Antigravity | `--mode plan` | `--mode accept-edits` | `--dangerously-skip-permissions`; same mechanism as unrestricted | `--dangerously-skip-permissions` |

These levels authorize an assignment; they do not make the five harnesses equally confined:

- **Claude:** restrictions use the harness permission layer, with no bridge OS sandbox. Accept-edits
  approves file edits, not arbitrary Bash or MCP calls. Publish adds only direct Git commit/push
  allowances; commands such as `gh pr create` still need their own approval rules. Deny rules win.
- **Codex:** Polybridge pins `approval_policy="never"`. Missing permission cannot be approved
  interactively. Workspace writes can include the checkout, temporary directories, configured
  writable roots, and the task scratch grant. There is no per-command commit deny list: a local
  commit can be mechanically possible at `write_in_repo`, even though publishing is not authorized.
- **OpenCode:** no bridge OS sandbox. The measured plan agent declined writes; that observation
  establishes model restraint, not guaranteed tool enforcement. Build already writes without
  `--auto`; auto-approval only affects requests that would otherwise ask and does not override denies.
- **Vibe:** no bridge OS sandbox. Plan restricts edit tools, but Bash still follows user rules;
  permissive Bash can allow mutations. Accept-edits approves edit tools but leaves Bash approval
  separate. Publish auto-approves tools broadly, rather than adding a narrow publishing exception.
- **Antigravity:** no bridge OS sandbox. Plan/accept-edits use the harness permission layer and
  settings. Builds/tests can be denied as mutating commands unless allowed. Publish bypasses the
  permission layer broadly, including operations outside the repository.

Write-capable workers receive `PB_TASK_SCRATCH`. Claude and Codex also receive an explicit grant for
that exact directory. A scratch path does not approve Bash commands or grant network access; other
adapters do not gain confinement or a directory grant merely because the path is provided.

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

### Codex

Complete Codex account/provider setup and inspect `~/.codex/config.toml` (or `CODEX_HOME`), including
selected profiles, configured writable roots, and MCP server/tool policies. Polybridge explicitly
selects the sandbox and disables interactive approvals; changing an interactive approval preference
does not make a blocked headless command approvable.

Use `write_in_repo` for edits/tests and set the saved network option when dependencies require it.
The bridge uses `sandbox_workspace_write.network_access` explicitly where applicable. Check reported
writable roots before assuming repository-only writes. Prepare credentials and approved tools for
publishing separately. The bridge mappings are documented in the [Codex adapter](../src/polybridge/backends/codex.py).

### OpenCode

Complete provider authentication and inspect global `~/.config/opencode/opencode.json` or
`opencode.jsonc` (`XDG_CONFIG_HOME` can relocate it), together with project and agent overrides.
Permit the specific shell/MCP operations required by the assignment; verify them with the installed
version's schema. Polybridge's `build` agent selection does not remove your configured denies.

Consult [OpenCode permissions](https://opencode.ai/docs/permissions/). The bridge's global MCP approval
editor supports the audited 1.x schema and refuses major version 2+ or an unverifiable CLI version;
it is not a universal configuration editor for every OpenCode release.

### Vibe

Prepare its provider credentials and model in `~/.vibe/config.toml` / provider environment, or the
directory selected by `VIBE_HOME`. Inspect project `.vibe/config.toml` and agent overrides as well:
Polybridge supplies `--trust`, so project configuration can affect a worker.

For **installed Vibe 2.25.8**, Bash uses literal command-prefix entries under `allowlist`. Merge
only the missing entries into the existing list and retain existing denies; this example illustrates
local Git inspection, not a replacement configuration:

```toml
[tools.bash]
permission = "ask"
allowlist = ["git status", "git diff", "git show"]
```

These prefixes also allow trailing arguments; they are not exact-command grants or glob patterns.
Use direct Git commands in the assigned working directory: Polybridge already sets it. Allowing
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
[Antigravity adapter](../src/polybridge/backends/antigravity.py) for measured permission caveats.
The bridge deliberately does not use agy's command sandbox, which did not establish usable,
consistent repository confinement in its recorded measurements.

## MCP approvals are separate from command permissions

Inspect a harness's global MCP approval entries without editing:

```bash
polybridge-ctl mcp-allowlist --backend vibe --json
```

After registering the server and confirming its credentials, explicitly approve a needed tool:

```bash
polybridge-ctl mcp-allowlist --backend vibe --allow jira/get_issue --json
```

Use your actual server/tool names. This command changes **global** harness configuration, backs up
existing content, and applies to future sessions; `--remove jira/get_issue` removes that approval.
It does not change Bash permissions, register the server, authenticate it, or enable network.
Project overrides and deny policies can still win. Claude/agy/Codex support `server/*`; Vibe requires
individual tools. Codex, Vibe, and OpenCode require an existing global server entry; OpenCode editing
also depends on its supported schema. See [approval adapters](../src/polybridge/backends/mcp_approval.py).

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
| Plan, screenshot or scratch output cannot be written | Check target path, writable roots and protected configuration paths. A command allow rule does not grant filesystem access. | Use `PB_TASK_SCRATCH` for temporary assignment output, or an explicitly authorized output directory. Keep plans out of protected settings directories. | One reversible file creation in the intended directory under the same access, followed by cleanup. |
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

Bridge mapping sources: [Claude](../src/polybridge/backends/claude.py), [Codex](../src/polybridge/backends/codex.py),
[OpenCode](../src/polybridge/backends/opencode.py),
[Vibe](../src/polybridge/backends/vibe.py), [Antigravity](../src/polybridge/backends/antigravity.py).

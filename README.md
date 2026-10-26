# HarflowMCP

<!-- mcp-name: io.github.Harflow-Dev/harflow-mcp -->

HarflowMCP is a local MCP server that dispatches coding tasks to Claude Code, Codex, opencode, Vibe, and Antigravity. It provides persistent task history, bounded reads, cancellation, continuation, and reusable workflows. On measured Claude Code versions, callers can opt in to approving individual Bash, Read, Write and Edit requests one exact action at a time (see [harness permissions](docs/harness-permissions.md#exact-action-caller-approvals)); on macOS with codex-cli 0.163.0 the same opt-in approves individual Codex commands and, with `caller_write_approvals`, patches through `codex app-server` (see [Codex caller approvals](docs/harness-permissions.md#codex-caller-approvals-codex-app-server)), and can grant a task a bounded list of specific Bash commands with `allowed_commands` instead of `unrestricted` access (see [dispatch-time command grants](docs/harness-permissions.md#dispatch-time-command-grants-allowed_commands)).

Current limits of these two features: `allowed_commands` is qualified only on Claude Code 2.1.310; caller approvals are qualified on Claude Code 2.1.302 (Bash approvals) and 2.1.310, and on Codex through `codex app-server` for codex-cli 0.163.0 on macOS only (commands, and patches with `caller_write_approvals`); other backends refuse them. Approvals are not available to native workflow children, and a request unanswered after 1800 seconds (or at the node's attempt deadline) is rejected by Harflow, never approved. After a denial there is no approve-later path: recovery is always a new `resume_task` or `continue_task` execution guided by `denial_remedy` (`allowed_commands` covers only Bash `approval_required` and chained-command parts; a denied Read, Write or Edit needs a resumed run so the agent can raise a fresh request; a `deny_rule` cannot be lifted by grants). Workflow permission-grant retry and approval notifications are not available. On compatible engines the Mac Monitor can answer caller-owned requests; authority follows the existing inferred, cooperative same-user model and does not authenticate a human or the app. See [harness permissions](docs/harness-permissions.md#exact-action-caller-approvals).

Vibe 3.0.1 headless dispatch uses its Rust CLI and the `ask` approval profile for `read_only`. This is an approval policy, not a write-tool exclusion: inherited policies or custom profiles can pre-approve writes. The retained `VIBE_CLI=python` selector is ignored by Vibe 3; interactive takeover inherits the user's environment.

Version 0.0.3 is a pre-release. [Harflow](https://github.com/Harflow-Dev/HarflowMac), the companion macOS application, is maintained separately and prepares its own exact paired engine runtime. Its managed runtime and MCP registrations work independently of the app process; see the [managed runtime guide](https://github.com/Harflow-Dev/HarflowMac/blob/main/docs/managed-runtime.md).

Detached task owners preserve output capture and live input across MCP server restarts.
Workflow scheduler recovery is explicit; see [supervision and recovery](docs/supervision.md).
Task status distinguishes the dispatching server, execution owner and responding server.
A normal attachment to a task launched by this server is not recovered; another observer or missing
identity is explained explicitly. Detail reads separately report dispatcher liveness, and routine
status polling provides a successor reference without scanning history. See [supervision and recovery](docs/supervision.md).
Git outcomes retain partial observations with per-probe failure and budget diagnostics; see
[observed task outcomes](docs/task-outcomes.md).
Task observations retry transient metadata replacements up to three reads and retain uncertainty
when metadata cannot be read reliably.
Recovered tasks disclose owner/process uncertainty and capture continuity. Qualified Claude
transcripts can supply bounded display-only recovery activity; see [public contracts](docs/contracts.md#supervision-and-recovery-activity).
Live-input capability does not promise that stdin is still open: status reports `input_channel`
and `input_closed_reason`. Wait for settlement before resuming a task whose input has closed.
MCP task status exposes paged successor links; resumes and continuations retain their source IDs.
Claude read-only plan mode permits built-in plan files; successful observed plan writes appear
as metadata-only task artifacts. Harness session history and logs can also persist outside the
checkout. See [harness permissions](docs/harness-permissions.md).
New launches and resumes require at least 256 MiB free on state, checkout and temporary-storage
filesystems. Confirmed-dead detached executions can resume their recorded conversation; live or
uncertain sessions remain protected. See [supervision and recovery](docs/supervision.md).
Backend discovery reports qualification gates for writable paths, caller approvals and command
grants. At unrestricted access, validated extra writable paths are disclosed as a no-op.
Live background wait bounds are persisted and configurable; see [task continuation](docs/task-continuation.md#live-background-wait-bound).

Use `harflow-ctl version-info --json` to inspect runtime identity and compatibility without initializing state. Engine operations require compatible Harflow state, task metadata, saved workflow definitions, workflow run documents and workflow ownership receipts. Known qualified storage lines migrate conservatively before new-line writes. Use `harflow-ctl state-inspect --json` to inspect an incompatible root without changing it, then explicitly confirm its `state-recover` proposal to retain a sibling backup and create fresh state. `HARFLOW_STATE_ROOT` can also select a separate fresh state directory; see [state ownership and recovery](docs/contracts.md#state-ownership-and-recovery) for compatibility requirements.

- [Getting started](docs/getting-started.md)
- [Workflows](docs/workflows.md), including acknowledged context sections and bounded instruction inspection
- [Task continuation](docs/task-continuation.md)
- [Task artifacts](docs/task-artifacts.md)
- [Task and workflow notifications](docs/task-notifications.md) (qualified Harflow database required)
- [Public contracts and runtime compatibility](docs/contracts.md)
- [Release validation and publication](docs/releasing.md)
- [Development and technical references](docs/development.md)

See the [Changelog](CHANGELOG.md) for change history.

Licensed under [Apache-2.0](LICENSE). Git history preserves source authorship; the license notice does not assert ownership of prior contributions.

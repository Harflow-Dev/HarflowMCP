# HarflowMCP

<!-- mcp-name: io.github.Harflow-Dev/harflow-mcp -->

HarflowMCP is a local MCP server that dispatches coding tasks to Claude Code, Codex, opencode, Vibe, and Antigravity. It provides persistent task history, bounded reads, cancellation, continuation, and reusable workflows. On measured Claude Code versions, callers can opt in to approving individual Bash, Read, Write and Edit requests one exact action at a time (see [harness permissions](docs/harness-permissions.md#exact-action-caller-approvals)); on macOS with codex-cli 0.163.0 the same opt-in approves individual Codex commands and, with `caller_write_approvals`, patches through `codex app-server` (see [Codex caller approvals](docs/harness-permissions.md#codex-caller-approvals-codex-app-server)), and can grant a task a bounded list of specific Bash commands with `allowed_commands` instead of `unrestricted` access (see [dispatch-time command grants](docs/harness-permissions.md#dispatch-time-command-grants-allowed_commands)).

Workflow callers can see every pending child question in status and wait replies and answer them independently by decision ID through the root run. Questions and structured clarification items remain complete, and forwarded answers return a durable child acceptance receipt. Resolved attention reasons disappear when execution resumes; unreadable workflow approval metadata is reported separately from requests needing a decision. See [workflow input and recovery](docs/workflows.md).

Current limits of these two features: `allowed_commands` is qualified only on Claude Code 2.1.310; caller approvals are qualified on Claude Code 2.1.302 (Bash approvals) and 2.1.310, and on Codex through `codex app-server` for codex-cli 0.163.0 on macOS only (commands, and patches with `caller_write_approvals`); other backends refuse them. Approvals are not available to native workflow children, and a request unanswered after 1800 seconds (or at the node's attempt deadline) is rejected by Harflow, never approved. After a denial there is no approve-later path: recovery is always a new `resume_task` or `continue_task` execution guided by `denial_remedy` (`allowed_commands` covers only Bash `approval_required` and chained-command parts; a denied Read, Write or Edit needs a resumed run so the agent can raise a fresh request; a `deny_rule` cannot be lifted by grants). Workflow permission-grant retry and approval notifications are not available. On compatible engines the Mac Monitor can answer caller-owned requests; authority follows the existing inferred, cooperative same-user model and does not authenticate a human or the app. See [harness permissions](docs/harness-permissions.md#exact-action-caller-approvals).

Vibe 3.0.1 headless dispatch uses its Rust CLI and the `ask` approval profile for `read_only`. This is an approval policy, not a write-tool exclusion: inherited policies or custom profiles can pre-approve writes. The retained `VIBE_CLI=python` selector is ignored by Vibe 3; interactive takeover inherits the user's environment.

Version 0.0.4 is a pre-release. [Harflow](https://github.com/Harflow-Dev/HarflowMac), the companion macOS application, is maintained separately and prepares its own exact paired engine runtime. Its managed runtime and MCP registrations work independently of the app process; see the [managed runtime guide](https://github.com/Harflow-Dev/HarflowMac/blob/main/docs/managed-runtime.md).

Detached task owners preserve output capture and live input across MCP server restarts.
Tasks can take turns on a named, exclusive resource (a simulator, a device, a database) with `resources`; a waiting task is acknowledged at once and reports `waiting_for_resources` until it spawns. A task can also run in its own linked Git worktree with `worktree`, leaving your checkout untouched. Workflows can declare run and node resource scopes and share one root-run worktree across nodes and linked child runs. See [resources and worktrees](docs/resources-and-worktrees.md). Every task also gets scratch folders shared across its resume chain and its group (`HARFLOW_CHAIN_SCRATCH`, `HARFLOW_GROUP_SCRATCH`), so outputs survive between runs; see [shared scratch](docs/shared-scratch.md).

Workflow scheduler recovery is explicit; see [supervision and recovery](docs/supervision.md).
Task status distinguishes the dispatching server, execution owner and responding server.
A normal attachment to a task launched by this server is not recovered; another observer or missing
identity is explained explicitly. Detail reads separately report dispatcher liveness, and routine
status polling provides a successor reference without scanning history. See [supervision and recovery](docs/supervision.md).
Git outcomes retain partial observations with per-probe failure and budget diagnostics. Commit
ranges include imported history and cannot identify task-created commits or authorship; see
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
grants, with network caveats returned as complete entries. At unrestricted access, validated extra writable paths are disclosed as a no-op.
Waits return for approvals that still need a decision or require reconciliation; a recorded
approval or rejection awaiting delivery does not by itself interrupt the next wait.
Live background wait bounds are persisted and configurable; see [task continuation](docs/task-continuation.md#live-background-wait-bound).

Use `harflow-ctl version-info --json` to inspect runtime identity and compatibility without initializing state. Engine operations require compatible Harflow state, task metadata, saved workflow definitions, workflow run documents and workflow ownership receipts. Known qualified storage lines migrate conservatively before new-line writes. Use `harflow-ctl state-inspect --json` to inspect an incompatible root without changing it, then explicitly confirm its `state-recover` proposal to retain a sibling backup and create fresh state. `HARFLOW_STATE_ROOT` can also select a separate fresh state directory; see [state ownership and recovery](docs/contracts.md#state-ownership-and-recovery) for compatibility requirements.

- [Getting started](docs/getting-started.md)
- [Workflows](docs/workflows.md), including acknowledged context sections and bounded instruction inspection
- [Task continuation](docs/task-continuation.md)
- [Resources and worktrees](docs/resources-and-worktrees.md)
- [Shared scratch folders](docs/shared-scratch.md)
- [Task artifacts](docs/task-artifacts.md)
- [Task and workflow notifications](docs/task-notifications.md) (qualified Harflow database required)
- [Public contracts and runtime compatibility](docs/contracts.md)
- [Release validation and publication](docs/releasing.md)
- [Development and technical references](docs/development.md)

History pages expose optional completion and ownership metadata for retained-history
summaries. Workflow completion time records the terminal status transition independently
of settlement; legacy and mixed-writer completion without a matching sequence witness remains unknown. See [compact reads](docs/compact-task-reads.md#retained-history-metadata)
for ownership, pinned workflow identity and completeness requirements.

Finished retained-history statistics are available to verified direct callers through
`harflow-ctl statistics-page --since=<ISO8601> --before=<ISO8601> --json`.
Clients require capability `statistics-page.v1`; older engines remain unsupported. Results
disclose retention, missing accounting and bounded preparation rather than assuming complete
history or zero usage. See [statistics contracts](docs/contracts.md#finished-retained-history-statistics).

See the [Changelog](CHANGELOG.md) for change history.

Licensed under [Apache-2.0](LICENSE). Git history preserves source authorship; the license notice does not assert ownership of prior contributions.

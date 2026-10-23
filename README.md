# HarflowMCP

<!-- mcp-name: io.github.Harflow-Dev/harflow-mcp -->

HarflowMCP is a local MCP server that dispatches coding tasks to Claude Code, Codex, opencode, Vibe, and Antigravity. It provides persistent task history, bounded reads, cancellation, continuation, and reusable workflows.

Version 0.0.1 is under development and unreleased. [Harflow](https://github.com/Harflow-Dev/HarflowMac), the companion macOS application, is maintained separately and prepares its own exact paired engine runtime. Its managed runtime and MCP registrations work independently of the app process; see the [managed runtime guide](https://github.com/Harflow-Dev/HarflowMac/blob/main/docs/managed-runtime.md).

Recovered tasks disclose owner/process uncertainty and capture continuity. Qualified Claude
transcripts can supply bounded display-only recovery activity; see [public contracts](docs/contracts.md#supervision-and-recovery-activity).
Live background wait bounds are persisted and configurable; see [task continuation](docs/task-continuation.md#live-background-wait-bound).

Use `harflow-ctl version-info --json` to inspect runtime identity and compatibility without initializing state. Engine operations require compatible Harflow state, task metadata, saved workflow definitions, workflow run documents and workflow ownership receipts. Use `HARFLOW_STATE_ROOT` to select a separate fresh state directory; see [state ownership and recovery](docs/contracts.md#state-ownership-and-recovery) for compatibility requirements.

- [Getting started](docs/getting-started.md)
- [Workflows](docs/workflows.md)
- [Task continuation](docs/task-continuation.md)
- [Task artifacts](docs/task-artifacts.md)
- [Task and workflow notifications](docs/task-notifications.md) (qualified Harflow database required)
- [Public contracts and runtime compatibility](docs/contracts.md)
- [Release validation and publication](docs/releasing.md)
- [Development and technical references](docs/development.md)

See the [Changelog](CHANGELOG.md) for change history.

Licensed under [Apache-2.0](LICENSE). Git history preserves source authorship; the license notice does not assert ownership of prior contributions.

# HarflowMCP

HarflowMCP is a local MCP server that dispatches coding tasks to Claude Code, Codex, opencode, Vibe, and Antigravity. It provides persistent task history, bounded reads, cancellation, continuation, and reusable workflows.

Version 1.0.0 is under development and unreleased. [Harflow](https://github.com/Harflow-Dev/HarflowMac), the companion macOS application, is maintained separately; native app integration is currently unavailable.

Use `harflow-ctl version-info --json` to inspect runtime identity and compatibility without initializing state. Engine operations require compatible Harflow state, task metadata and saved workflow definition documents. Use `HARFLOW_STATE_ROOT` to select a separate fresh state directory; see [state ownership and recovery](docs/contracts.md#state-ownership-and-recovery) for compatibility requirements.

- [Getting started](docs/getting-started.md)
- [Workflows](docs/workflows.md)
- [Task continuation](docs/task-continuation.md)
- [Task artifacts](docs/task-artifacts.md)
- [Public contracts and runtime compatibility](docs/contracts.md)
- [Development and technical references](docs/development.md)

See the [Changelog](CHANGELOG.md) for change history.

Licensed under [Apache-2.0](LICENSE). Git history preserves source authorship; the license notice does not assert ownership of prior contributions.

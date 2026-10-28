# Getting started

HarflowMCP requires Python 3.11+ and the agent CLIs you intend to use. Version 0.0.4 is unreleased; install from this source checkout:

```sh
uv tool install . --force --no-cache
harflow-setup --status
harflow-setup
```

`harflow-setup --status` inspects client registrations. Run `harflow-setup` to register the server with supported MCP clients, or use `harflow-setup --dry-run` to preview registration commands.

The commands are `harflow-server`, `harflow-ctl`, and `harflow-setup`. The MCP registration key is `harflow`. State defaults to `~/.harflow`; set `HARFLOW_STATE_ROOT` to a nonempty absolute path to select another root. Tasks use `<root>/tasks`. The override does not change HOME or vendor CLI configuration discovery. Existing Polybridge state is not imported. Invalid overrides fail before state creation; see [state layout and errors](contracts.md#selected-state-layout). The first engine operation initializes a fresh root. Nonempty unmarked roots and incompatible stores are refused; choose a new empty root rather than editing ownership files. See [state ownership and recovery](contracts.md#state-ownership-and-recovery).

Ordinary headless agent dispatch is supported. Native workflow adapters and extra writable path grants are currently unavailable; see [current qualification limits](development.md#current-qualification-limits).

## Guides

- [Workflows](workflows.md)
- [Task continuation](task-continuation.md)
- [Task outcomes](task-outcomes.md)
- [Task artifacts](task-artifacts.md)
- [Task notifications](task-notifications.md)
- [Waiting for multiple tasks](multi-task-waiting.md)
- [Compact task reads](compact-task-reads.md)
- [Harness permissions](harness-permissions.md)
- [Extra writable paths](extra-writable-paths.md)

# polybridge

polybridge is a local [MCP](https://modelcontextprotocol.io) server that hands coding tasks to
headless coding agents on your machine and returns at once. You start a task, get a `task_id` back
straight away, and check on it, wait for it or continue it whenever you like. The caller is never
blocked while the agent works.

Every agent sits behind the same interface. An assistant like Claude Desktop can therefore send work
to Claude Code, Codex, opencode, Mistral Vibe or Antigravity, run several in parallel, and compare
what they produce.

## Supported agents

| Backend | CLI | Notes |
|---|---|---|
| `claude` | [Claude Code](https://docs.claude.com/en/docs/claude-code) | turn cap, cost reporting, live messages while it runs |
| `codex` | [Codex CLI](https://github.com/openai/codex) | real OS sandbox; network can be blocked or allowed |
| `opencode` | [opencode](https://opencode.ai) | cost reporting |
| `vibe` | [Mistral Vibe](https://github.com/mistralai/mistral-vibe) | model chosen in its own config only |
| `antigravity` | Google Antigravity CLI (`agy`) | live messages while it runs |

Each agent has to be installed and logged in separately. polybridge only drives what is already on
your `PATH`. To see what is installed and what each agent can actually do, call `list_backends`.

## Install

Requires macOS or Linux with Python 3.11+. [`uv`](https://docs.astral.sh/uv/) is installed for you
if it is missing.

```bash
git clone <this repo> && cd polybridge
./install.sh
```

This installs the server and registers it with the Claude desktop app and with every supported agent
CLI it finds. The desktop app can't be reliably detected, so its entry is written either way. Restart
the desktop app afterwards; the CLIs pick it up on their next session.

```bash
polybridge-setup --dry-run                 # show what would change
polybridge-setup --client claude-desktop   # register with only some clients
polybridge-setup --status                  # is each client registered and up to date?
polybridge-setup --uninstall               # remove the registration again
```

After changing the code, reinstall with `uv tool install . --force --no-cache`.

## Usage

From any MCP client:

1. **`start_task(prompt, repo_path, backend="claude", freedom="write_in_repo")`** dispatches the task
   and returns a `task_id`. Optional: `model`, `reasoning_effort` (`low` … `xhigh`), `max_turns`,
   `network`, `title`.
2. **`wait_for_task(task_id)`** waits up to about a minute. If the task is still running when it
   returns, nothing is wrong; just call it again. **`get_task_status(task_id)`** returns the same
   information without waiting.
3. **`resume_task(task_id, followup_prompt)`** continues the same conversation as a new task.

Also available: `list_tasks`, `cancel_task`, `get_task_events` (the task's activity log, page by
page), and `send_message` (add a message to a running claude or antigravity task).

### How much the agent may do

| `freedom` | Meaning |
|---|---|
| `read_only` | look, don't touch |
| `write_in_repo` (default) | edit the repository |
| `publish` | may also try to commit, push or open a PR |
| `unrestricted` | no restrictions from polybridge |

Each agent enforces these differently. Only Codex has a real OS sandbox; the others rely on the
agent's own permission system. Every task reports an `enforcement` block that says what was actually
enforced. Check it rather than relying on the level's name.

### From the terminal

```bash
polybridge-ctl list                 # recent tasks
polybridge-ctl status <task_id>
polybridge-ctl run --backend codex --repo . --prompt "…"
polybridge-ctl takeover <task_id>   # continue a task's session in the agent's own UI
```

Tasks are stored under `~/.polybridge/tasks/` and survive server restarts.

### Monitor app (macOS)

`macos/` contains a SwiftUI menu-bar app that shows tasks live and lets you start, message, cancel
and take over sessions. Build it with `macos/build-app.sh`.

### Workflows

Workflows combine a saved visual graph with an agent orchestrator. Add Planning, Implementation,
Review, and Task steps, connect them with conditions, and configure parallel branches, bounded
loops, Resume/Fresh sessions, and ordered fallback agents. Polybridge owns the saved definition
and execution state; the agents do not install or manage workflow files.

The Monitor's Workflow screen switches between a live graph and the existing Parallel activity
columns. Planning produces a task checklist; only the orchestrator can mark its items complete.
The menu bar shows workflow status and opens individual runs alongside existing agent activity.

Use `workflow_builder` to generate an editable draft, or save a definition with `save_workflow`.
Start it with `start_workflow`, or add `workflow="name"` to `start_task`. Workflow dispatch returns
a `workflow_run_id`; use `get_workflow_status` / `wait_for_workflow` to follow it rather than the
ordinary task-status tools. Existing task calls without `workflow` are unchanged.

See [workflow definitions, execution, and recovery](docs/workflows.md) for the CLI, graph format,
and fallback behavior.

## Development

```bash
uv sync
uv run pytest                                          # unit tests: fast, no accounts needed
PB_INTEGRATION=1 uv run pytest -m integration           # real agent runs (costs money)
PB_CLI_INTEGRATION=1 uv run pytest -m cli_integration   # real CLIs, sandboxed configs
uv run mcp dev src/polybridge/server.py                 # MCP Inspector
```

For the Monitor app, run `swift build && swift test` in each package under `macos/`, and run
`swiftformat macos && swiftlint lint` before committing.

Contributor notes — the architecture, invariants and measured CLI behaviour — are in
[CLAUDE.md](CLAUDE.md).

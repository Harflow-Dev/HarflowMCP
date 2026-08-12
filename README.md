# polybridge

A local MCP server that dispatches coding tasks to **whichever headless coding agent you want** —
Claude Code, Codex or opencode — and returns immediately, so the caller is never blocked for the
length of a run.

Same async contract for every backend: `start_task` hands back a `task_id`, then you poll it, await
it with a non-destructive timeout, or continue the same session with follow-up instructions.

## Why this exists alongside claude-code-bridge

`claude-code-bridge` does one agent well and is the simple thing to hand a team. This one is
multi-backend, because the alternatives for Codex were both wrong:

- `codex mcp-server` exposes only `codex` and `codex-reply`, and **both block until done** — so a
  long task dies on the client's request timeout with no `task_id` to come back to.
- `owlex` has the right shape but hard-times-out at 300s and returns whole raw transcripts.

## Install

```bash
./install.sh
```

Then restart the Claude desktop app. Preview the config change first with
`polybridge-setup --dry-run`.

## Tools

| Tool | Blocking? | What it does |
|---|---|---|
| `list_backends()` | No | What's installed, its version, and what each backend can actually do |
| `start_task(prompt, repo_path, backend, freedom, model, max_turns)` | No | Dispatches, returns a `task_id` immediately |
| `get_task_status(task_id)` | No | Status, summary, turns, usage, denials, enforcement, stream tail |
| `wait_for_task(task_id, timeout_seconds=55)` | Until done or timeout | On timeout returns `running` and **leaves the run alone** |
| `resume_task(task_id, followup_prompt)` | No | Continues that session as a **new** task |
| `list_tasks(status, backend)` | No | All tasks, oldest first, optionally filtered |
| `cancel_task(task_id)` | Until dead | SIGTERM the process group, SIGKILL after 5s |

## Backends are not interchangeable, and the tool says so

| | claude | codex | opencode |
|---|---|---|---|
| we can choose the session id | ✅ | ❌ (it mints and reports one) | ❌ (`ses_…`, reported on the first event) |
| turn cap (`max_turns`) | ✅ | ❌ — asking for it is an **error**, not silently ignored | ❌ — same |
| dollar cost reported | ✅ | ❌ token counts only, `total_cost_usd` is null | ✅ per step, summed across the run |
| **real OS sandbox** | ❌ | ✅ `read-only` / `workspace-write` | ❌ |
| **per-command deny** | ✅ `git commit`/`git push` | ❌ none | ❌ none |

One `freedom` parameter expresses intent — `read_only`, `write_in_repo` (default), `unrestricted` —
and is mapped to each backend's real mechanism. Because those mechanisms differ in strength, every
task reports what was **actually** enforced rather than what the parameter implies:

```json
"enforcement": {
  "freedom": "write_in_repo",
  "mechanism": "codex sandbox: workspace-write",
  "os_enforced": true,
  "writes_confined": true,
  "writable_roots": ["the working directory", "/tmp", "$TMPDIR"],
  "commit_push_blocked": false,
  "direct_commit_commands_denied": false,
  "caveats": ["writes are confined by the OS, but to the workspace *plus* temporary directories …"]
}
```

Every boolean there is a **strict** claim — true only if the named thing genuinely cannot happen.
Two consequences worth understanding:

- `writes_confined: true` for Codex does **not** mean repo-only. Codex permits
  `[workdir, /tmp, $TMPDIR]`, which is why `writable_roots` spells it out.
- Claude reports `commit_push_blocked: false` even though its deny patterns refuse `git commit`,
  because `git -C … commit` and `bash -c 'git commit'` get through (measured). The weaker, true
  statement lives in `direct_commit_commands_denied: true`.

Claude also reports `os_enforced: false` and `writes_confined: false` throughout: it has no sandbox,
and `repo_path` is only its working directory.

opencode reports every enforcement boolean as `false` at every level, because its `freedom` mapping
is only a choice of agent (`plan` for `read_only`, `build` otherwise) and none of it is an OS
boundary. Two measured consequences live in its caveats: `plan` *declined* to write or run commands
rather than being observed to be prevented from doing so — it never attempted a write, so no
tool-layer refusal was exercised — and `build` wrote a file and ran a shell command **without
`--auto`**, so `--auto` is not what separates writing from not writing, and how much `unrestricted`
adds over `write_in_repo` depends on the user's own opencode configuration.

## Tasks outlive the server process

An MCP client may run several polybridge servers, or restart one. Each task writes a
`<task_id>.meta.json` beside its stream log under `~/.polybridge/tasks/`, so any server can report
on, wait for, cancel or resume tasks it did not start. Those come back marked `recovered: true` with
a `note` describing what is known.

## Development

```bash
uv sync
uv run pytest                                   # unit: no auth, no tokens
PB_INTEGRATION=1 uv run pytest -m integration    # real agent runs; costs real money
uv run mcp dev src/polybridge/server.py          # MCP Inspector
```

# polybridge

A local MCP server that dispatches coding tasks to **whichever headless coding agent you want** —
Claude Code, Codex, opencode or vibe — and returns immediately, so the caller is never blocked for
the length of a run.

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

That installs the server and registers it with **the Claude desktop app plus each agent CLI found**. Preview
everything it would do, without changing anything, with `polybridge-setup --dry-run`.

| Client | Where the entry goes | How it gets there |
|---|---|---|
| Claude desktop app | `claude_desktop_config.json` | edited here — the app has no CLI |
| Claude Code | `~/.claude.json`, user scope | `claude mcp add` |
| Codex | `~/.codex/config.toml` | `codex mcp add` |
| opencode | `~/.config/opencode/opencode.jsonc` | `opencode mcp add` |
| vibe | `~/.vibe/config.toml` | `vibe mcp add` |

The four CLIs write their own config because each stores the entry in its own shape — opencode's key
is `environment`, not `env`, and its `command` is an array — and because their files are not ours to
reformat: one is 78 KB of application state, another is hand-commented TOML, one is JSONC, and one is
TOML again but not ours to reformat either — measured, `vibe mcp add` rewrites the whole file and
destroys any hand-written comments in it, unlike codex's and opencode's own `add`, which preserved
theirs.

Only the desktop app needs a restart. The CLIs read their config when a session starts.

The desktop app is the one entry written without checking for the client first: it can be installed
anywhere, and its config directory does not exist until it has run once, so requiring either would
skip a freshly installed app. The four CLIs are genuinely detected, by looking for their binary.

```bash
polybridge-setup --client codex,opencode   # register with only these
polybridge-setup --dry-run                 # print what would be written or run
polybridge-setup --status                  # is each client registered, and is it current?
polybridge-setup --uninstall               # remove the registration from each client
polybridge-setup --status --json           # the same, as one versioned JSON document
```

`--status` and `--uninstall` do not need the server binary, so they still work after it has been
removed. `--status` changes nothing and runs nothing except `codex mcp list --json`: Claude Code's
own `mcp get` launches the server, so its `~/.claude.json` (or `$CLAUDE_CONFIG_DIR/.claude.json`) is
read directly, as are the desktop app's JSON, opencode's JSONC and vibe's TOML. An entry is
`current` only if both its command and its PATH match what install would write now.
`--dry-run` applies to install only and is rejected with the other two.

`--uninstall` removes only our own entry. The desktop app's file is backed up first, as on install.
vibe's `mcp remove` rewrites `config.toml` and strips its comments, so that file is backed up with a
timestamp before vibe is asked, and the report names the backup. opencode has no `mcp remove`: the
report names the file to edit, and nothing is changed.

`--json` prints `{"v": 1, "server_path": …, "clients": [{"key", "available", "installed",
"command", "current", "action", "error", "notes"}]}` and nothing else on stdout. `installed` and
`current` are `null` when they cannot be said — the client is absent, the check failed, or there is no
server binary to compare with — never `false` by default. `action` is what this run did to that
client (a status below), or `null` for `--status`. The shape changes only with `v`.

Each client is reported separately, and one failing never stops the others. The seven statuses
distinguish things that are easy to conflate:

- `applied` — the desktop config was written, or a CLI's own `add` command exited zero. For the CLIs
  that is all that was observed: it does not mean the client has loaded the server or can launch it,
  which is why the report says "add command succeeded" rather than "registered".
- `previewed` — `--dry-run`; nothing was touched.
- `skipped` — that client's binary isn't on PATH, so there was nothing to register with.
- `failed` — it did not work, and the report says what ran and what came back.
- `unknown` — a CLI timed out, or answered in words it does not recognise. It may already have
  written its config, so calling it `failed` would be a guess stated as a fact.
- `removed` — `--uninstall` removed the entry (for a CLI: its `remove` command reported so).
- `not_installed` — `--uninstall` found nothing to remove.

A non-zero exit from `polybridge-setup` therefore means "not everything was confirmed", not
"something definitely failed". Each action judges that for itself: `--status` exits 1 only when a
check could not be completed, never because a client is absent; `--uninstall` treats `not_installed`
and opencode's manual step as success, and exits 1 on any `failed` or `unknown`.

Updating Claude Code is the one destructive step. Its `mcp add` refuses to overwrite and has no
`--force`, so an existing entry is removed first. If the replacement then fails, the report says to
assume polybridge is not registered — the removal succeeded, but a failed `add` is not proof it wrote
nothing — and prints the command that restores it.

### Registering it with the agents it dispatches to

Claude Code, Codex, opencode and vibe are all backends *and* clients here, so an agent dispatched by
polybridge can call polybridge and dispatch further agents. Nothing bounds that nesting; the only
brake is the one-live-run-per-session guard, which stops a session resuming itself but not a new
session being started. If you don't want it, leave those clients out: `polybridge-setup --client
claude-desktop`.

## Tools

| Tool | Blocking? | What it does |
|---|---|---|
| `list_backends()` | No | What's installed, its version, and what each backend can actually do |
| `start_task(prompt, repo_path, backend, freedom, model, max_turns, reasoning_effort, network, group)` | No | Dispatches, returns a `task_id` immediately |
| `get_task_status(task_id)` | No | Status, summary, turns, usage, denials, enforcement, stream tail |
| `wait_for_task(task_id, timeout_seconds=55)` | Until done or timeout | On timeout returns `running` and **leaves the run alone** |
| `resume_task(task_id, followup_prompt, max_turns, network)` | No | Continues that session as a **new** task; `network` omitted inherits the parent's, an explicit boolean overrides it |
| `list_tasks(status, backend)` | No | All tasks, oldest first, optionally filtered |
| `cancel_task(task_id)` | Until dead | SIGTERM the process group, SIGKILL after 5s; cascades to live descendants and reports them under `cascade` |
| `send_message(task_id, text)` | No | Adds a message to a running **live-input** task (claude, no `max_turns`); returns `queued`, never `delivered` |

## Backends are not interchangeable, and the tool says so

| | claude | codex | opencode | vibe |
|---|---|---|---|---|
| we can choose the session id | ✅ | ❌ (it mints and reports one) | ❌ (`ses_…`, reported on the first event) | ❌ (mints a UUID, present on the very first stream entry) |
| we can choose the model | ✅ | ✅ | ✅ | ❌ — model selection is config-only there; `model` is refused before dispatch |
| turn cap (`max_turns`) | ✅ | ❌ — asking for it is an **error**, not silently ignored | ❌ — same | ✅ — but a breach exits 1 and is reported as `failed`, not a clean stop like claude's |
| dollar cost reported | ✅ | ❌ token counts only, `total_cost_usd` is null | ✅ per step, summed across the run | ❌ — the stream carries no cost **and no token counts at all** |
| **real OS sandbox** | ❌ | ✅ `read-only` / `workspace-write` | ❌ | ❌ |
| **per-command deny** | ✅ `git commit`/`git push` | ❌ none | ❌ none | ❌ none |
| live input (`send_message`) | ✅ unless `max_turns` is set | ❌ | ❌ | ❌ |

One `freedom` parameter expresses intent — `read_only`, `write_in_repo` (default), `publish`,
`unrestricted` — and is mapped to each backend's real mechanism. Because those mechanisms differ in
strength, every task reports what was **actually** enforced rather than what the parameter implies:

```json
"enforcement": {
  "freedom": "write_in_repo",
  "mechanism": "codex sandbox: workspace-write + -c sandbox_workspace_write.network_access=false",
  "os_enforced": true,
  "writes_confined": true,
  "writable_roots": ["the working directory", "/tmp", "$TMPDIR"],
  "commit_push_blocked": false,
  "direct_commit_commands_denied": false,
  "publish_attempts_allowed_by_polybridge": false,
  "network_access": "blocked",
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

## `publish`: authorizing an attempt, not promising it succeeds

`publish` sits between `write_in_repo` and `unrestricted`. Two fields on `enforcement` exist just for
it: `publish_attempts_allowed_by_polybridge` is `true` only at freedoms (`publish`, `unrestricted`)
where polybridge **authorized an attempt** to commit, push or open a PR — an authorization claim
only, which says nothing about whether the *mechanism* prevents the attempt elsewhere (codex at
`write_in_repo` with `network=True` can mechanically reach a remote with this field false) nor
whether the attempt succeeds — it is **not** called `publishing_permitted`, because that name would
read as a promise polybridge cannot make. Credentials, remote permissions, branch protection, repo
hooks, and an unauthenticated `gh` can all still stop the attempt, and it says nothing about
whether the agent even tries. `network_access` is one of `"blocked"`, `"enabled"`, `"unrestricted"`
or `"not_controlled"` — the last means polybridge imposes nothing of its own and the surrounding
environment decides, which is **not** the same as `"blocked"`.

Where codex reports `"blocked"` it passes `-c sandbox_workspace_write.network_access=false`
explicitly rather than relying on the sandbox default, and the `mechanism` string names it. That is
not belt-and-braces: a plain `workspace-write` run defers the setting to the user's own
`config.toml`, and was measured reaching the network (HTTP 200) on a config that enables it. The
explicit override makes the claim true of the run instead of the machine.

The freedom ladder is a *requested ordering*, not a guarantee that every backend implements four
distinct strengths — two backends collapse an adjacent pair on purpose:

| backend | `publish` mechanism | vs. its neighbours |
|---|---|---|
| claude | `--permission-mode acceptEdits`, deny patterns dropped, `--allowedTools "Bash(git commit:*),Bash(git push:*),Bash(gh pr create:*)"` added | genuine middle tier — everything not allow-listed still needs approval, so is still refused headlessly (measured: dropping only the deny patterns was NOT enough on its own) |
| codex | `-s workspace-write` **plus** `-c sandbox_workspace_write.network_access=true` | genuine middle tier, OS-enforced — but the switch opens **general** network access, not git-specific |
| opencode | `--agent build`, same as `write_in_repo` | **collapses into `write_in_repo`**: nothing here was ever enforced, so `publish` adds nothing real |
| vibe | `--agent auto-approve`, same as `unrestricted` | **collapses into `unrestricted`**: `accept-edits` left a measured `git commit` auto-denied, so `auto-approve` is the only profile that can publish, and it removes every other restriction too |

Those two collapses are intentional and covered by tests pinning the byte-identical argv, not a gap —
`assert_safe` genuinely cannot distinguish `write_in_repo` from `publish` on opencode, or `publish`
from `unrestricted` on vibe, because there is no argv difference to check.

Claude's `unrestricted` also changed behaviour to make this level make sense: the deny patterns used
to apply even at `unrestricted`, refusing ordinary `git commit`/`git push` there too. They are now
dropped at `unrestricted` (bypassPermissions with no denies) — a **compatibility break** for anyone
who relied on the old behaviour. That it actually lets a commit through was measured the same way as
the rest: `bypassPermissions` with no `--disallowedTools`, against a scratch repo with a bare remote,
committed and pushed with `permission_denials` empty.

opencode reports every enforcement boolean as `false` at every level, because its `freedom` mapping
is only a choice of agent (`plan` for `read_only`, `build` otherwise, `build` again for `publish`) and
none of it is an OS boundary. Two measured consequences live in its caveats: `plan` *declined* to
write or run commands rather than being observed to be prevented from doing so — it never attempted a
write, so no tool-layer refusal was exercised — and `build` wrote a file and ran a shell command
**without `--auto`**, so `--auto` is not what separates writing from not writing, and how much
`unrestricted` adds over `write_in_repo` (or `publish`) depends on the user's own opencode
configuration.

vibe reports every enforcement boolean as `false` at every level too — it has no OS sandbox
(`os_sandbox=False`), so `os_enforced` and `writes_confined` are `false` throughout, and whether
`git commit`/`git push` are even reachable depends on the user's own `[tools.bash]` allowlist, not on
polybridge, so `per_command_deny`, `direct_commit_commands_denied` and `commit_push_blocked` are all
`false`. `read_only` maps to `--agent plan`, which does set `write_file` and `edit` to
`permission: "never"` — a real refusal, not model restraint — **but leaves `bash` untouched**, so a
user whose own config allows `bash` unconditionally could still write through it; the caveat says so
rather than implying `read_only` is airtight. `write_in_repo` maps to `--agent accept-edits`
(auto-approves `write_file`/`edit` only) and `publish`/`unrestricted` both map to `--agent
auto-approve` (`bypass_tool_permissions: true`) — see the collapse table above for why `publish`
cannot be narrower than `unrestricted` here. In programmatic mode every approval callback vibe doesn't
pre-approve is auto-denied, so anything outside the agent profile and the user's own config simply
fails closed rather than prompting.

## `network`: asking for the network without climbing to `publish`

`start_task(..., network=...)` and `resume_task(..., network=...)` take an optional boolean that
asks for network access independently of the freedom. `None` (the default) keeps each freedom's
historical behaviour exactly — byte-identical argv on every backend. `True` asks polybridge to
impose no network barrier of its own; `False` asks it to impose one. The parameter governs
**polybridge's own network barrier, never reachability** — a corporate firewall or proxy defeats
it too, and on claude/opencode/vibe the surrounding environment always decides.

Only codex has a barrier polybridge can actually raise or lower
(`-c sandbox_workspace_write.network_access=…`), and its support is non-rectangular — measured
with the free `codex sandbox` harness (codex-cli 0.154.0), which runs a command under the real
sandbox with no model call:

| codex freedom | default | `network=True` | `network=False` |
|---|---|---|---|
| `read_only` | blocked | **error** — the key is inert under the read-only sandbox (measured) | accepted — already blocked by the sandbox itself, no pair emitted |
| `write_in_repo` | blocked | `-c …network_access=true` | `-c …network_access=false` |
| `publish` | enabled | `-c …network_access=true` | `-c …network_access=false` |
| `unrestricted` | open | accepted — no sandbox left to configure | **error** — no barrier remains to raise |

The domain-allowlist machinery codex carries (`experimental_network.domains`, legacy
`allowed_domains`/`denied_domains`) is inert via `-c` — measured, a curl to a "denied" host
returned 200 — so no domain-scoped tier is buildable today.

Two identities follow on codex and are pinned by tests: `write_in_repo`+`network=True` produces
an argv byte-identical to `publish`'s default, and `publish`+`network=False` produces one
identical to `write_in_repo`'s default. The first means a push can genuinely reach a remote at
`write_in_repo` although publishing was not authorized — the difference is recorded intent, not
an enforced barrier — and it enables arbitrary outbound traffic, exfiltration included, which is
why `assert_safe` cannot refuse the crossed claim and the dispatch-time branch notice discloses
it instead. The second means `publish`+`network=False` blocks **network-backed** push only:
measured, `git push <local bare repo> HEAD:refs/heads/main` still landed with
`network_access=false`.

On claude, opencode and vibe, `network=True` is accepted — "impose no barrier" is genuinely
delivered by having nothing to impose — and `network=False` is an error rather than being
silently dropped; `enforcement.network_access` stays `"not_controlled"` there and a bridge
notice says polybridge imposed nothing.

On `resume_task`, `network=None` inherits the parent run's setting and an explicit boolean
overrides it for the new run only.

## Reasoning effort is one vocabulary, passed through verbatim

`start_task(..., reasoning_effort=...)` accepts `"low"`, `"medium"`, `"high"` or `"xhigh"` and hands
it to whichever backend ran, on that backend's own flag — no translation, because each of those four
level names is spelled the same way, and accepted, on the three backends that support reasoning
effort at all — claude, codex, opencode (per each CLI's own `--help` text and model metadata). That
is a claim about spelling only, not about the levels behaving identically or forming an equivalent
ladder across backends — see the acceptance/behaviour caveats below.

vibe is the exception, and it is why "no translation" cannot be said of all four: its native ladder is
`off/low/medium/high/max`, with no `xhigh` at all, and vibe's own OpenAI-responses backend maps its
`max` to `xhigh` internally. The vocabularies do line up — a faithful `low→low, medium→medium,
high→high, xhigh→max` translation would be correct — but vibe would be the first backend needing a
translation rather than a verbatim pass-through, and that is only one of the reasons reasoning effort
is unsupported there outright (see the capability table above and CLAUDE.md's vibe section for the
config-precedence reason it can't be done safely). A request for any level, on vibe, is refused before
dispatch, the same treatment `model` gets there.

| backend | flag | value |
|---|---|---|
| claude | `--effort <v>` | as-is |
| codex | `-c model_reasoning_effort="<v>"` | as-is |
| opencode | `--variant <v>` | as-is |
| vibe | — (no flag exists; config-only) | unsupported outright — refused before dispatch |

An effort outside those four levels is refused before a task is dispatched, on every backend that
supports the parameter — never silently dropped or degraded. That matters because each CLI mishandles
an unsupported value differently if it ever reached one directly: claude silently degrades to its
default effort with only a stderr warning; opencode silently ignores it with no warning at all; codex
alone fails loudly, as a mid-run API `400`; vibe has no flag to mishandle in the first place — an
unsupported value there would only ever reach `[[models]].thinking` in config, several layers removed
from anything polybridge controls.

`xhigh` is deliberately the ceiling, not necessarily each backend's true maximum. For claude and
codex it genuinely is not: each has a native tier above `xhigh` (claude `max`; codex `max`/`ultra`)
that stays unreachable through polybridge on purpose, so a caller cannot spend it by accident.
opencode is different — on its variant-capable models, `xhigh` **is** the top declared variant, so
there is no higher tier being deliberately withheld there; only the low end (`minimal`) is
unreachable, for no reason beyond keeping one vocabulary all three share:

| backend | unreachable through polybridge |
|---|---|
| claude | `max` |
| codex | `none`, `minimal`, `max`, `ultra` |
| opencode | `minimal` |
| vibe | all four — `low`, `medium`, `high`, `xhigh` — reasoning effort is unsupported outright |

Within **codex**, `xhigh` is also the one tier every model measured accepts (per
`~/.codex/models_cache.json`) — `ultra`/`max` are absent from some codex models, and stopping short
of them removes a model-dependent `400` as a possibility entirely there. That acceptance is not a
universal claim across backends: opencode's own `--variant` support is per model, so a model can lack
`xhigh` (or any other level) just as easily as codex's outliers lack `ultra`/`max` — see below.

opencode's support is real but **per model**, and polybridge cannot know which model has variants:
measured working on the free `opencode/muse-spark-1.3-contributor-free`, where reasoning tokens on
one prompt rose with the variant over three paired runs each — 172–230 at `low` vs 239–308 at
`xhigh`, and 75–99 at the native `minimal` below them. The paid `ling-3.0-flash-fin` accepts only
`low`, `medium` and `high` (no `xhigh` at all), and a model that declares no `variants` in `opencode
models --verbose` — `big-pickle`, for example — ignores `--variant` with no warning at all. So
`levels_change_behaviour: true` in `list_backends`' `capabilities.reasoning_effort` for opencode means
"shown to change behaviour on one capable model," not "guaranteed for whichever model this run uses"
— the gap is spelled out in that block's own `caveats`, never left implied by the boolean alone. The
weaker `accepted_in_real_run: true` claims only that a real run accepted the flag — the CLI neither
refused it nor silently degraded it to a default — which is all claude's and codex's evidence
amounts to; see their own caveats for what that leaves open.

## Tasks outlive the server process

An MCP client may run several polybridge servers, or restart one. Each task writes a
`<task_id>.meta.json` beside its stream log under `~/.polybridge/tasks/`, so any server can report
on, wait for, cancel or resume tasks it did not start. Those come back marked `recovered: true` with
a `note` describing what is known.

## Nested dispatches: lineage, caps and cascade cancel

An agent running under a polybridge task can itself call polybridge — a nested MCP server inside a
`claude` or `opencode` run, or a pre-approved tool call from `codex` or `vibe`. Polybridge records
that relationship on the child: `spawned_by` (the calling task), `root_task_id`, `depth` (0 for a
root task), `max_depth` (taken from the root's `PB_MAX_DEPTH`, default 2), an optional `group` label
(a `start_task` parameter, inherited by nested dispatches), and `lineage_detected` — which method
found the caller, or null if none was found. All of them appear in `get_task_status`, `list_tasks`
and `polybridge-ctl`. `parent_task_id` keeps its meaning of "resumed from".

**Detection is best-effort.** Every spawned agent gets `PB_TASK_ID`, `PB_ROOT_TASK_ID` and
`PB_DEPTH` in its environment, but codex and vibe filter the environment before starting their MCP
servers, so those never arrive there. The caller is therefore looked for three ways, each candidate
confirmed alive by pid and start time: the `PB_TASK_ID` candidate (only if it is also this
process's session or an ancestor), a task whose process group is this process's session (claude,
opencode and codex nested servers share it), and a walk up the process tree (vibe gives each MCP
call its own session). A task with no detected caller is treated as a root task.

**Caps.** When a caller is detected, a nested `start_task` or `resume_task` is refused unless the
child's resolved `enforcement` is at least as strict as the caller's on every policy field:
`os_enforced`, `writes_confined`, `commit_push_blocked` and `direct_commit_commands_denied` (true
on the parent means true on the child); `publish_attempts_allowed_by_polybridge` (false stays
false); `network_access` (blocked > enabled = not_controlled > unrestricted); and, under a
confined parent, the same backend, `writable_roots` a subset of the parent's, and a repo equal to or
under the parent's — so codex `read_only` cannot spawn codex `write_in_repo`, and a confined parent
cannot spawn a different backend at all. `depth + 1` may not exceed `max_depth`. The error names the
rule that was hit. This is a cap on what polybridge will dispatch, not a sandbox: an agent that can
run commands can still start processes outside polybridge, and a caller that detection misses is
not capped.

**Cascade cancel.** `cancel_task` stops the task and every live descendant it can find, following
`spawned_by` through settled intermediates and every task sharing its `root_task_id`, re-scanning
until no new descendant appears. Tasks this server owns are cancelled as before. A task owned by
another live server is signalled, then left to that server to settle; if it has not within the
SIGKILL and drain grace periods and its server is still alive, it is reported under
`owner_still_settling` and nothing is written for it. A task whose owning server is confirmed dead is
signalled, SIGKILLed if it survives, and written `cancelled`. The response's `cascade` lists
`cancelled_descendants`, `sigkill_survivors`, `owner_still_settling`, `not_signalled` (with why —
a pre-start-time record is only signalled when `ps` actually showed its markers) and `not_recorded`
(signalled, but its `cancelled` status could not be written — the record is left untouched). It re-scans at
most 5 rounds; if the last round still found new descendants it scans once more, and anything new
there — never signalled — is listed in `unconverged` with `cascade_incomplete: true` (otherwise
`false` and `[]`). A takeover refuses an incomplete cascade as `descendants_not_stopped`.

**Why a cancel from another process cannot be mis-recorded.** Every cancel writes phase files beside
the task's record, per attempt: `<id>.cancel.<n>.req` (who is cancelling, with a 60 s lease) before
any signal, then `.sig` (with whether the leader was alive when signalled) or `.failed`. The owning
server, seeing its process exit with a `.req` in flight, waits for the outcome instead of guessing:
only a `.sig` that found the leader alive turns that exit into `cancelled`, even if the agent caught
SIGTERM and exited 0. A `.failed`, or a canceller that died with its lease expired, leaves the normal
classification in place. A second canceller that joins an attempt already under way first records an intent
file (`<id>.cancel.<n>.join-<token>`), then resolves it with the shared `.sig` or its own `nosig-<token>`;
a `.failed` only settles the attempt once no such intent is outstanding, so a delivery whose `.sig` is
still being written can never be outrun by it.

**When in doubt, it waits rather than guesses.** A canceller checks the leader's identity (pid and
start time) immediately before every signal and records `leader_alive` from that check alone. Any
phase or intent file that cannot be read or listed, or a phase that cannot be written, is treated
as undecided. The price is deliberate: a disk that keeps failing, or someone tampering with
`~/.polybridge/tasks/` by hand, can leave an owner's monitor waiting and keep a task's record past
retention — until the disk recovers, or the canceller exits and its 60 s lease expires. The
alternative, settling on missing evidence, would record a cancel that never happened or lose one
that did. Resumes of one session are serialised by a lock under
`~/.polybridge/sessions/`, so two servers cannot both resume it.

## The normalized event log

Alongside a task's raw stream log (`<task_id>.jsonl`, whatever bytes the backend's CLI actually
produced) and its record (`<task_id>.meta.json`), each task also writes
`<task_id>.events.jsonl` — one JSON object per line, in a shape that means the same thing across
every backend rather than each one's own stream format. Every line carries an envelope:
`v` (schema version, currently `1`), `seq` (0-based, starting at `task_started`), `observed_at`
(when polybridge wrote the line), `source_ts` (the backend's own timestamp for the event, when it
has one), `raw_offset` (byte offset into the raw stream log the event was derived from, null for
`task_started`/`task_finished` and for any event recorded while the raw log itself could not be
written), `task_id`, and `kind` — one of `task_started`, `task_finished`, `assistant_text`,
`assistant_delta`, `tool_call`, `tool_result`, `user_message`, `usage`, `notice`, or `undelivered`.
`assistant_delta` is one streamed chunk of claude text (`message_id`, `block_index`, `text`); the
final `assistant_text` for that block carries the same `message_id`/`block_index`. That set is
closed (`events.EVENT_KINDS`): writing any other kind raises, and a test pins the set to this list
and to every emit site, because the Monitor app switches on it — a new kind is a contract change.
On a live-input
task every message sent to the agent is a `user_message` — the prompt with `source: "initial"`, each
`send_message` with `source: "injected"` and its `message_id` — and one that never reached it is an
`undelivered` event with the same `message_id` and a `reason`.

This file is written only by the server process that owns the task — never by a recovered task's
new server, and never by `polybridge-ctl`. So a task recovered after its original server died has
an events log that simply stops where that server's did, exactly like its raw stream log; there is
no owner left to keep appending to it.

## Live input: talking to a claude task while it runs

A claude task started without `max_turns` runs with `--input-format stream-json` and an open stdin
(`live_input: true` on the task — read that field, not the backend name). `send_message` — or
`polybridge-ctl send` from another process — queues a message for it:

- while a turn is running, the message is **folded into that turn** (one result answering both);
- if the agent is idle, it starts a new turn;
- once the agent is idle with nothing queued and no background task open, polybridge closes its
  stdin, so the task still exits and settles on its own and `wait_for_task` is never held open.

A send is refused once input has closed, with **"finished; continue with resume_task"** — which is
the thing to do. It is also refused for a task without live input, a settled one, one whose process
has already exited, and one whose owning server is not confirmed alive. It only ever returns `queued`: the event log's
`user_message` / `undelivered` events say what actually happened. If a result reports an error the
task stops forwarding at once, and every message still queued is reported `undelivered` with a
notice rather than silently dropped.

`max_turns` with live input has never been measured, so a capped task keeps the one-shot shape and
cannot take messages. A task that started a background job keeps its input open until the job
finishes (claude runs a turn for it by itself); one that waits on background jobs alone, with no
output, for `PB_LIVE_IDLE_SECONDS` (default 600) has its input closed anyway — which kills the jobs
— and is reported `failed`, never a clean completion.

The owning server delivers the messages. If it restarts mid-run the turn in flight still finishes
(the agent is its own session leader), but messages queued and not yet written are lost with it,
and a background job still open is killed when the agent's stdin reaches EOF.

Two caveats worth knowing before relying on it:

- **"queued" on the owning server means in memory.** A `send_message` handled by the server that
  owns the task goes into that server's in-memory queue, not onto disk; the pump normally writes it
  within a second. If that server dies abruptly in between, the message is lost and **no event
  records it** — neither `user_message` nor `undelivered`. (A `polybridge-ctl send`, or a send from
  a different server, is appended to the on-disk inbox instead, and is reported either way.)
- **A local process holding the inbox lock can hold a live task open.** Every send and every close
  take an `flock` on `<task_id>.inbox.jsonl`. A process that takes that lock and then stalls — or a
  hostile local process — keeps an idle live task from closing its stdin for as long as it holds
  it, because closing without the lock could accept a message after the last forward. The server's
  event loop stays responsive and other tasks are unaffected; only that task waits. Cancelling it
  still works.

## `polybridge-ctl`: a CLI over the same records

```bash
polybridge-ctl list [--since 7d] [--json]
polybridge-ctl status <task_id> [--json]
polybridge-ctl send <task_id> <text> [--json]
polybridge-ctl cancel <task_id> [--json]
polybridge-ctl takeover <task_id> [--json]
polybridge-ctl takeover-attach <task_id> --pid <pid> [--json]
polybridge-ctl run --backend B --repo R --prompt P [--freedom F] [--model M] [--max-turns N]
                   [--reasoning-effort E] [--network true|false] [--group G] [--json]
polybridge-ctl resume <task_id> <text> [--max-turns N] [--network true|false] [--json]
```

`list` and `status` read exactly what the MCP tools read — `store` and `tasks.default_log_dir()` —
and never construct a `TaskRegistry`. `send` appends to a live-input task's inbox under the inbox
lock for the owning server to deliver, with the same refusals as `send_message`. No command ever
starts retention.

- **`status`**'s `task` document (and `get_task_status`) carries `resume_command`: a ready-to-paste
  `cd <repo> && <argv>` string that resumes the task's session in a POSIX shell (bash/zsh — not
  `cmd.exe`/PowerShell), built the same way for a live task and a recovered one by
  `backends.resume_command`. `null` when no session id has been disclosed yet, the repo path is not
  absolute, or the backend has no safe interactive resume for it.
- **`cancel`** runs the same cascade as `cancel_task`, from a registry the ctl process owns, and
  returns `{task_id, status, cascade}`.
- **`run` / `resume`** fork a detached process that owns the new task until it settles (it drains
  the agent's output and records the outcome). The command itself returns as soon as the task
  exists: `{"task_id"}` (exit 0), or `{"error": …}` (exit 1) for anything that stopped it —
  validation (`run` accepts exactly what `start_task` accepts), the nested-dispatch caps, a busy
  session, a spawn failure. If the owner says nothing within 30 s it is stopped (cancelling anything
  it had started) and the answer is `{"v": 2, "unknown": {...}}` with exit 3 — a task may or may not
  exist, so check `polybridge-ctl list`. The owner logs to `~/.polybridge/ctl.log`; SIGTERM/SIGINT to
  it cancel its task. Neither command opens the Monitor app.
- **`takeover`** is for a person at the Monitor, never an agent: it refuses when `PB_TASK_ID` is set
  or a calling task is detected — and also when detection cannot establish that there is none
  (`caller_undecidable`: `ps` missing or denied, an unreadable ancestry or session, a related task
  whose liveness cannot be decided) — because the interactive session runs under the user's own default
  permissions, not the task's `freedom`. It reserves the session, stops a live headless run (cascade
  cancel, then confirms the process is gone — a survivor, or a run whose liveness cannot be decided,
  refuses), and returns `{argv, cwd, session_id, note}`: the absolute command that resumes the
  session in that CLI's own interactive UI. A task that had already finished keeps its status. Every
  refusal after the reservation is recorded, so a retry starts a fresh attempt.
- **`takeover-attach`** records the terminal's process within 120 s of the takeover. From the
  takeover until that window lapses unattached, or until the attached process is confirmed gone,
  `resume_task` on the session is refused with the usual "session busy" error. A takeover that has
  not yet reached `.ready` stays busy past the 120 s window for as long as its `polybridge-ctl`
  controller is alive — a deliberate divergence from the plan's window alone, so a slow cascade
  cancel cannot let a resume start on the session before the interactive command is handed out.
  Snapshots and
  listings of a taken-over task carry `taken_over: true` and `taken_over_note`.

| Backend | Interactive command handed out by `takeover` |
|---|---|
| claude | `claude --resume <session_id>` (claude's own folder-trust dialog in an untrusted folder) |
| codex | `codex -c check_for_update_on_startup=false resume <thread_id>` (codex's own trust prompt) |
| opencode | `opencode <repo> -s <session_id>` |
| vibe | `vibe --trust --workdir <repo> --resume <session_id>` |

`--json` output is always exactly one document on stdout, versioned with its own `CTL_JSON_VERSION`
(currently 2 — a separate contract from `polybridge-setup`'s and the event log's, which each stay at
their own v1):
`{"v": 2, "tasks": [...]}` for `list`, `{"v": 2, "task": {...}}` for `status`,
`{"v": 2, "result": {...}}` for the others, `{"v": 2, "unknown": {...}}` for an unanswered `run`/
`resume`, and `{"v": 2, "error": {"code": ..., "message": ...}}` on failure. `--since` accepts a
duration like `7d`, `12h`, or `30m`. Diagnostics go to stderr, never stdout, so a script parsing
`--json` output never has to filter noise out of it.

## Opening the Monitor app

On macOS, a root task — no detected caller and no `PB_TASK_ID` — started through the MCP server runs
`open -g polybridge-monitor://task/<id>` in the background. It never blocks or changes the dispatch:
a failure becomes a notice on the task, and the `start_task` response never claims the app opened.
`PB_OPEN_MONITOR=0` turns it off (the test suite sets it, and so does everything the app launches).

## The Monitor app (macOS)

`macos/PolybridgeMonitor/` is the root Swift package (macOS 14+, SwiftUI): a regular Dock app with
one window and a menu bar item that shows polybridge tasks live and lets a person act on them; it
keeps running after the window closes. It also guards against two copies running at once: if
another non-terminated copy with the same bundle identifier is already running from a **different**
bundle path (e.g. an installed copy and a freshly built one), a second launch normally hands over to
the running one and quits — forwarding any `polybridge-monitor://` URLs it was opened with, or
requesting a plain reopen if it was a bare launch — instead of leaving two copies open. This is
best-effort, not guaranteed: a copy opened by hand may flash briefly before quitting, and two truly
simultaneous launches are not guaranteed to resolve to one. Its UI layer follows a
coordinator/VM/use-case architecture, one SwiftPM package per module, wired by path —
see `macos/AGENTS.md` for the architecture rules and each package's own `AGENTS.md` for what that
package owns:

```
macos/
  PolybridgeMonitor/    the app shell: App.swift, AppDelegate, AppCoordinator, AppModulesRegistry
  PbFoundation/         PbUtilities, PbCommon, PbUI — generic helpers, the architecture contract layer, UI tokens/components
  PbCore/               MonitorCore (I/O, no dependencies), PbRepository
  PbFeatures/           MainWindowFeature, MenuBarFeature, SettingsFeature — one package per screen group
```

To browse every package in one Xcode window, open `macos/PolybridgeMonitor.xcworkspace`. SwiftUI
previews run from the library packages (pick a feature or PbUI scheme with My Mac as the destination),
not from the `PolybridgeMonitor` executable scheme. The app itself is still built with `macos/build-app.sh`.

Every package under `macos/PbFoundation/`, `macos/PbCore/` and `macos/PbFeatures/` builds and tests
on its own, and so does the root package itself:

```bash
swiftformat macos && swiftlint lint                        # format, then lint (SwiftFormat 0.62.1, SwiftLint 0.65.0)
scripts/check-private-refs.sh                              # the private-reference guard CI runs
for p in macos/PbFoundation/* macos/PbCore/* macos/PbFeatures/* macos/PolybridgeMonitor; do
  (cd "$p" && swift build && swift test)                  # MonitorCore etc.; no GUI needed
done
macos/build-app.sh                                         # builds macos/build/Polybridge Monitor.app
rm -rf ~/Applications/'Polybridge Monitor.app' && cp -R 'macos/build/Polybridge Monitor.app' ~/Applications/
```

Lint rules live in `.swiftlint.yml` and `.swiftformat` at the repo root. MonitorCore's sources move
unchanged, so their existing violations are recorded in `.swiftlint-baseline.json`; any new
violation still fails.

**CI.** `.github/workflows/lint.yml` runs SwiftFormat (`--lint`), SwiftLint and
`scripts/check-private-refs.sh` on Linux. `.github/workflows/test.yml` runs `swift test` in every
Monitor package (one matrix entry each), `macos/build-app.sh`, and `uv run pytest` with a temporary
`HOME`/`CODEX_HOME`, all on macOS runners. The headless app smoke is a local gate only.

`build-app.sh` only builds (ad-hoc signed, not sandboxed, bundle id `dev.polybridge.monitor`);
installing is the copy above, and opening it once from `~/Applications` registers the
`polybridge-monitor://` scheme. It also best-effort unregisters the build copy from LaunchServices
after building, so task links resolve to an installed copy rather than silently launching the build
copy again; if nothing is installed yet, the scheme simply has no handler until the app is opened
once from `~/Applications`.

What it reads and runs — it never writes polybridge's own state:

- **Lists and status** only from `polybridge-ctl list/status --json`, refreshed on FSEvents for
  `*.meta.json` under `~/.polybridge/tasks/` (debounced to 1 s), plus a slow poll while anything runs.
- **Live timelines** by tailing `<task_id>.events.jsonl` (v1) for the tasks on screen, by byte
  offset; unknown kinds are ignored. Titles come from each log's first line.
- **Summary** from the agent's own report, never git: its final answer, refusals/warnings, the
  files its own edit tools reported touching (paired `tool_call`/`tool_result` events by
  `call_id` — a shell command that edits a file is invisible here), usage/cost, and what was
  enforced.
- **Actions** only through `polybridge-ctl` (`send`, `resume`, `cancel`, `run`, `takeover`,
  `takeover-attach`) and `polybridge-setup` (Settings → Harnesses; Install/Remove only on click).
- **Tools** are found in the folder set in Settings, then `uv tool dir --bin`, `~/.local/bin`,
  `/opt/homebrew/bin`, `/usr/local/bin`. Everything it launches gets the login-shell `PATH`, no
  `PB_*` variables, and `PB_OPEN_MONITOR=0`. A `polybridge-ctl` that lacks a subcommand or answers
  another `"v"` is reported as too old, never guessed at.
- **Take over** always opens Terminal.app — there is no embedded terminal in the app. It hands the
  argv `takeover` returns over as NUL-separated data files (never shell text) read by a fixed script,
  which attaches its own pid (`takeover-attach`) before `exec`ing the CLI in that same pid; if the
  attach is refused the script exits without starting the session.

When the app notices `polybridge-ctl`/`polybridge-setup` are missing or out of date, it offers to
install polybridge itself — a banner in the sidebar, the menu bar, and Settings → Harnesses, with
an "Install polybridge" (or "Update polybridge") button. The source is always GitHub
(`git+https://github.com/hainayanda/Polybridge.git`, unpinned), installed with `uv`; if `uv` isn't
found the app offers to install that too, with its own confirmation, using astral's official
installer. Installing needs system `git` (via the Command Line Tools) as well. The app only ever
imports the login shell's `PATH` for this — a shell-only `UV_*` setting has no effect on what the
app does.

## Retention

A running server sweeps settled task records at most once every 24 hours, controlled by
`PB_RETENTION_DAYS` (default 30 days; `0` disables the sweep entirely). A task is only deleted once
it is terminal, older than the window, has no still-running descendant (from `resume_task` or a nested dispatch), and no
cancel/takeover attempt still in flight (a cancel attempt is finished once it has a `.sig` or
`.failed`, or its canceller died and its lease expired; a takeover attempt once it no longer holds
its session) — and only `polybridge-server` ever runs it;
`polybridge-ctl` never triggers a sweep. Lock files — `<task_id>.lock` and the live-input inbox
`<task_id>.inbox.jsonl` — are never deleted.

**A deleted task's id stops working for `resume_task` — with one exception.** `resume_task` checks
its own in-memory registry before falling back to the on-disk record, so a server that still holds
the task as a live Python object (never evicted, because eviction only happens once the registry is
over capacity) can keep resuming it even after retention deletes its record and logs. Any other
server — including the same one after a restart, or once that task is finally pruned from memory —
sees no record at all and reports it as an unknown task id, the same as if it had never existed.

## Development

```bash
uv sync
uv run pytest                                          # unit: no auth, no tokens
PB_INTEGRATION=1 uv run pytest -m integration           # real agent runs; costs real money
PB_CLI_INTEGRATION=1 uv run pytest -m cli_integration   # real client CLIs, sandboxed configs; free
uv run mcp dev src/polybridge/server.py                 # MCP Inspector
```

The two opt-ins are separate on purpose. `cli_integration` spends nothing, but it depends on optional
external binaries and on their current flag and output shapes — which is what the default suite
promises not to do.

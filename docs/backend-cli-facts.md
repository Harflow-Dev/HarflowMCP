# Backend CLI facts

Behavior of the agent and client CLIs that Harflow drives, measured on this machine. The facts were
measured on the predecessor engine. They are evidence of each CLI's behavior, not certification of
HarflowMCP, and `docs/development.md` remains the maintained statement of current limits.

Read this before changing argv, flags, stdin wiring, stream parsing, classification or registration
in `backends/` or `clients/`. When a backend, CLI version or freedom level changes, re-measure
rather than reasoning about it, and record the new measurements here. Do not "tidy" these away.

**Claude Code (2.1.260)**
- `-p --output-format stream-json` refuses to start without `--verbose`.
- `--disallowedTools` is variadic — patterns must be one comma-separated value.
- `--max-turns` works but is undocumented in `--help`.
- Never symlink-resolve the `claude` path: `~/.local/bin/claude` points into a versioned directory,
  so resolving it pins `PATH` to today's version and breaks at the next update.
- `--effort <level>` accepts `low, medium, high, xhigh, max`. **An invalid value is silently
  degraded**: stderr gets `Warning: Unknown --effort value '<v>' — ignoring it and using the default
  effort`, and the run proceeds anyway — the CLI never refuses to start. This is why Harflow
  validates `reasoning_effort` itself rather than passing an unrecognised value through: a dropped
  request would otherwise look like a run that honoured it.
- **Dropping the deny patterns does NOT let a run commit — the lever is `--allowedTools`.** This was
  the plan's assumed mechanism and it is wrong. Measured, three paired runs in a scratch repo with a
  local bare remote:
  - with `--disallowedTools "Bash(git commit:*),Bash(git push:*)"`: both refused,
    `Permission to use Bash with command ... has been denied.`
  - with those patterns simply **omitted**: still refused, but a *different* message —
    `This command requires approval`. `acceptEdits` auto-approves *edits*, not arbitrary Bash, and
    headless `-p` has nobody to approve, so the command dies either way.
  - with `--permission-mode acceptEdits --allowedTools "Bash(git commit:*),Bash(git push:*)"`: both
    **succeeded**, `permission_denials` empty, and the commit landed in the bare remote.
  So the `publish` freedom drops the git denies *and* adds the allow entries; passing both would be
  self-defeating, since deny beats allow. `--allowedTools` is variadic exactly like
  `--disallowedTools`, so its patterns are likewise one comma-separated value.
- **`-p` is a boolean flag and the prompt is a separate positional**, so a prompt token equal to a
  real option name is parsed by claude as that option. The prompt therefore rides after a `--`
  separator, as on codex and opencode. Measured both ways: without `--`, a prompt of
  `--dangerously-skip-permissions` reached claude's own parser and aborted for lack of a prompt;
  with it, the same text was delivered literally. Also measured on **resume** —
  `--resume <id> -- "<option-shaped prompt>"` preserved the session id and delivered the text
  verbatim, so the separator holds on both paths. That is the **classic** shape, which claude now
  uses only when `max_turns` is set (see live input below).
- **Live input: `--input-format stream-json`, a stdin pipe, and the prompt as the first stdin
  line** — `{"type":"user","message":{"role":"user","content":[{"type":"text","text":…}]}}`.
  Measured on 2.1.281 (`tests/test_live_input_real.py`): next to `--input-format stream-json` **a
  positional prompt is silently ignored** (exit 0, no model call), so the live shape has no `--` and
  no positional at all. A message written mid-turn is folded into that turn (one `result`); after a
  `result` the process idles until more input or EOF; EOF after a result exits 0 in ~1.2 s; a
  message written just before EOF still runs as its own turn; `--resume` takes the same shape.
  `total_cost_usd` is cumulative per process, `num_turns`/`usage`/`permission_denials` per result.
  `--max-turns` with live input is unmeasured, so capped runs remain one-shot. Large capped
  assignments over 32 KiB use `--input-format text`, raw UTF-8 stdin and immediate EOF, with no
  positional prompt. Native Claude 2.1.289 start and persisted-session resume were verified against
  an isolated local fake API with byte-identical 1 MiB prompts and the turn cap retained.
  **`assert_safe` accepts these three shapes and validates each completely**: classic is `--`
  plus one positional, no `--input-format`, stdin DEVNULL, no initial input; live is no `--`, every
  token a known option (so no positional), `--input-format stream-json` exactly once, no
  `--max-turns`, a pipe, and an `initial_input` that is exactly one well-formed user line. It takes
  the whole `Invocation`; capped text stdin requires a turn cap, `pipe_once`, nonempty UTF-8 input,
  and no positional or live-input flags. A live argv wired to DEVNULL (or a classic one to a pipe) is refused, and
  a bare argv list is refused outright.
- **`--include-partial-messages` streams text as it arrives (measured 2.1.283, real captures in
  `tests/fixtures/claude_partial_*.jsonl`) — in the classic `-p` shape, in the live shape, and on
  `--resume`.** The stream gains `stream_event` lines: `message_start` (carries the message id),
  `content_block_start` (carries the block's `index`), `content_block_delta` (`text_delta` carries
  a text chunk; `thinking_delta`, `signature_delta` and `input_json_delta` carry nothing
  Harflow surfaces), `content_block_stop`, `message_delta`, `message_stop`. Claude still emits
  one `assistant` event **per content block** — after that block's deltas, before its
  `content_block_stop` — so a streamed chunk's identity is `(message_id, block_index)`, and the
  `result` line is unchanged. Subagent `stream_event`s carry a non-empty `parent_tool_use_id` like
  other subagent events. Harflow normalizes the deltas to an additive `assistant_delta` event
  kind, with optional `message_id`/`block_index` on `assistant_text` — a new *kind* is additive
  under `EVENT_LOG_VERSION` 1 (older apps decode it as `.unknown`), so the version does not bump.
- **Background tasks (measured, 2.1.281).** `system/task_started` carries `is_backgrounded` — and a
  *foreground* Bash emits `task_started` too, with `is_backgrounded: false`, plus its own
  `task_notification`, so only `is_backgrounded: true` counts. On finish: `system/task_updated`
  with `patch.status: "completed"`, then `system/task_notification` `status: "completed"`; with
  stdin still open claude then **starts a follow-up turn by itself** and emits another `result`.
  EOF while one runs kills it: `patch.status: "killed"`, notification `status: "stopped"`, no
  further result, exit 0 — nobody ever sees that task's output. EOF at the moment of the
  notification still lets the follow-up turn finish.
- **The approval layer refuses a chained command citing each part separately** — a run that bundled
  `git commit -am wip` with `echo "EXIT: $?"` was refused naming both — so an allow-listed command
  is still refused when it arrives chained to something else.
- `bypassPermissions` with the denies dropped permits commit and push (measured the same way:
  empty denials, commit in the remote). Note this makes `unrestricted` a **behaviour change** —
  before the `publish` work the denies were passed at every level, and deny beats bypass, so
  `unrestricted` did *not* permit an ordinary `git commit` despite its name.

**Codex (codex-cli 0.153.2)**
- **Extra writable paths, macOS codex-cli 0.162.0-alpha.17.2, 2026-10-14.** Actual isolated
  fake Responses API probes verified directory/existing-file grants with workspace-write,
  fresh/resume/CLI-child inheritance, hostile ambient roots and both network settings. Pin
  `sandbox_workspace_write.writable_roots=[]`, `exclude_slash_tmp=true` and
  `exclude_tmpdir_env_var=true` with canonical `--add-dir` grants. Ambient roots otherwise merge.
  Exact-file replacement is permitted during execution; saved identity changes reject later
  reuse. Unrelated/sibling/symlink-escape writes were OS-denied. Ordinary CLI children inherit
  these boundaries; guessed disable flags did not suppress spawning. Harflow's separate native
  adapter remains uncertified for these grants. See `docs/extra-writable-paths.md` and the no-paid
  `tests/test_extra_path_cli.py`; do not extrapolate to another CLI/platform/configuration.
- **`codex exec` waits for EOF when reading stdin.** Small prompts use DEVNULL. Prompts over
  32 KiB use the documented `-` positional with `Invocation.stdin_mode="pipe_once"`: deliver the
  full UTF-8 assignment and immediately close stdin, with no live-input pump. A fake executable
  verifies byte-identical delivery of a 1 MiB assignment and EOF. This keeps results complete
  without exceeding the operating system argv limit. Backend validators distinguish this
  one-shot transport from a live conversation pipe.
- An approval prompt would hang a headless run equally, hence pinned `-c approval_policy="never"`.
- Prompt goes after `--`, so prompt text can never be parsed as an option.
- The stream is nothing like Claude's: session id is **`thread_id`** on `thread.started`; the final
  answer is an `item.completed` with `item.type == "agent_message"`; there is **no terminal
  success/failure event** and **no dollar cost**, only token counts on `turn.completed`.
- An `item.type == "error"` was observed in a *successful* run — error **items** are notices. A
  **top-level** `{"type": "error"}` event is a real failure. Do not conflate them.
- `workspace-write` permits `[workdir, /tmp, $TMPDIR]` — it is **not** repo-only.
- `-c model_reasoning_effort="<level>"` is not validated by the CLI at all; an unsupported value
  reaches the API as a mid-run `400`/`turn.failed` rather than being rejected up front. Confirmed
  honoured end to end: a run at `"ultra"` (outside Harflow's four-level vocabulary) recorded
  `"reasoning_effort":"ultra"` in its own session rollout.
- `codex -C <dir>` into an untrusted directory was recorded here as failing outright on 0.153.2.
  **Re-measured on the same version and it does not, at least for `codex exec`:** a fresh
  `/tmp` directory, `git init`-ed and never trusted (absent from `~/.codex/config.toml`), ran
  normally under `-s read-only`, and a second fresh directory ran under `-s workspace-write` too.
  So a sandboxed test on a fresh `-C` target is not expected to fail for trust reasons. The
  original note may have been about the interactive CLI rather than `exec`, which was not
  re-tested — so this narrows the claim rather than deleting it.
- **`workspace-write` defers network to the user's own config; `sandbox_workspace_write.network_access`
  is the switch.** Measured with one `curl https://example.com` per sandbox, on a config that does
  not set the key: `read-only` → blocked (`curl: (6) Could not resolve host`); `workspace-write` →
  blocked, same error; `workspace-write` plus `-c sandbox_workspace_write.network_access=true` →
  **HTTP 200**; `danger-full-access` → HTTP 200.
  **That "blocked at workspace-write" is a property of the config, not the sandbox** — measured
  again with `[sandbox_workspace_write] network_access = true` in the user's own `config.toml`, a
  plain `workspace-write` run reached the network (HTTP 200). So Harflow passes an explicit
  `...=false` wherever it reports `blocked` (measured: that returns it to exit 6), and names the
  override in the `Enforcement.mechanism` string. `read-only` was measured immune to the key even
  with it set true, so it takes no override. The first version of this note generalised from one
  clean machine — the same mistake as the vibe `mcp add` measurement below. That is what makes `publish` a genuine tier on codex rather than a relabelling: without
  it a push cannot reach a remote at all.
  The key name came from the binary's own strings — it carries both `[sandbox_workspace_write]` and
  the sentence *"In `workspace-write`, network access still depends on your Codex configuration (for
  example `[sandbox_workspace_write] network_access = true`)"* — which mattered, because **codex does
  not validate `-c` keys at all** (see `model_reasoning_effort` above): a guessed key would have been
  silently ignored, and "network still blocked" would have been indistinguishable from "wrong key".
  The switch opens **general** network access, not git or `gh` specifically — anything inside the
  sandbox can reach the network — so the enforcement block says that rather than implying it only
  unlocks pushing.
- **`codex exec resume` rejects `-C`/`-s`: they are global `codex exec` options, absent from `codex
  exec resume --help`.** Measured on codex-cli **0.154.0**, 2026-09-16 (the rest of this Codex
  section was measured on 0.153.2 and was not re-measured here). Using a bogus thread id so a parse
  success shows up as `no rollout found for thread id …` rather than an option error:
  `codex exec --json -C <dir> -s read-only -c approval_policy="never" resume -- <id> <prompt>`
  parses; `codex exec resume --json -C <dir> …` (the shape Harflow shipped) does not — `error:
  unexpected argument '-C' found`, exit 2, before any model call. So `build_resume_argv` places the
  option region *before* `resume`, with `resume` as the region's own last token, immediately before
  `--`. The shipped shape was never observed to work: it failed this way in the field (a reported
  resume, exit 2 in 0.119s), it fails on 0.154.0 here, and the same rejection was recorded for `-s`
  on 0.145.0 — three points, not a proof about every version in between.
- **`item.started`/`item.completed` `file_change` items are now normalized, not dropped.** The raw
  item is `{"type":"file_change","changes":[{"path","kind"}],"status":"in_progress"|"completed"}` —
  previously `_normalize_item_started`/`_normalize_item_completed` handled only
  `command_execution`/`mcp_tool_call`, so an edit codex made through this item type never reached
  the Monitor's event stream at all (measured against real `~/.harflow/tasks` logs while adding
  the Monitor's Summary tab, 2026-09-26). Each change now gets its own `tool_call`
  (category `edit`, tool `file_change`, `call_id` = `"<item id>:<path>"` — path, not list index, so
  pairing survives the changes list being reordered between start and completion) and, on
  completion, a `tool_result` with `ok = (item's own status == "completed")` — codex reports no
  per-change status, only one for the whole item. A completion-only item (no prior `item.started`)
  synthesizes its `tool_call`s first, exactly like the shell/MCP completion paths already did.
- **Native workflow children (initially measured 0.160.1, 2026-10-06; re-certified on
  0.162.0, 2026-10-09).** The installed CLI uses
  `collaboration.spawn_agent` with `task_name`, `message`, and `fork_turns="none"`, followed by
  `collaboration.wait_agent`. It has no child-close tool. Child `SubAgentActivity` and terminal
  evidence are persisted in session rollouts but are omitted from the exec JSON stream, so the
  adapter finalizes from bounded, nonce-correlated parent/child evidence after the control process
  exits. Parent acknowledgement alone is never worker success.
- **An `agents={...}` override deep-merges ambient roles.** A hostile `agents.default.config_file`
  replaced the child's model despite the explicit parent model. Native invocations therefore
  override the selected default role with the packaged `backends/codex_native.toml` profile, pinning
  `gpt-6.1-sol`, the selected certified access profile and approval policy `never`. Read-only
  workers use `codex_native.toml`; repository writers use `codex_native_write.toml`. The real CLI
  fixture verifies the exact model and provider, inherited effort, distinct sequential and parallel
  children through persisted parent resume, a refused nested spawn, denied read-only shell writes,
  permitted repository writes and blocked network. The operative nested
  spawn refusal is the thread limit; disabling feature flags alone does not establish that the
  installed runtime removes its collaboration tools. Re-measure rather than assuming the public
  release source or a feature name describes this installed binary.
- Certification uses `HARFLOW_CLI_INTEGRATION=1 uv run pytest tests/test_codex_native_subagent_cli.py`
  against an isolated localhost fake API, with no paid model calls. Native support remains limited
  to CLI 0.162.0, model `gpt-6.1-sol`, fresh sequential or bounded parallel workers, and read-only
  or repository-write access with blocked network. Child access must match its owning session;
  read-only children cannot narrow a write owner. No turn-cap support is added.

**opencode (1.18.18)**
- `run --format json` emits clean JSONL — `step_start`, `tool_use`, `text`, `step_finish`, `error` —
  with `sessionID` on every event, so the id is known from the first line.
- **`cost` on `step_finish` is per step, not cumulative.** A three-step run reported 0.0583 / 0.0149
  / 0.0148; the run cost 0.0881. Assigning instead of accumulating understates it fourfold. Token
  counts are per step for the same reason.
- `step_finish.reason` is `tool-calls` mid-run and `stop` on the last step. That is the *only*
  end-of-run signal — a `text` part can appear at any step, so "saw some text" cannot tell a finished
  run from one killed mid-flight.
- A top-level `error` event comes with exit code 1. There is no terminal success event.
- `--` is honoured; `-s <id>` resumes into the same session id (both verified).
- Assignments over 32 KiB use EOF-closed one-shot UTF-8 stdin, with trailing `--` and no argv
  message, on both start and resume. Installed CLI source reads `Bun.stdin.text()` and returns
  that text unchanged when the argv message is empty; leading/trailing whitespace and CRLF remain
  intact. Fake executable checks verify byte-identical 1 MiB delivery and EOF without live input.
  Small assignments retain the separated argv shape.
- Its parser also accepts `--flag=value` and compact `-sID`, and **a later value wins**: appending
  `--format=default` after `--format json` disabled the JSON stream outright (measured). So
  `assert_safe` walks the option region and refuses any non-canonical token instead of searching for
  flags — a search sees one `--format` and passes while opencode honours the other one.
- No grandchild process: the server runs in-process, so the CLI leader is the only pid.
- **`--agent build` writes and runs commands without `--auto`** (measured: it created a file and ran
  `git status` unprompted). So `--auto` is not what separates writing from not writing. That is all
  that run establishes — it does *not* show what `build` permits in general, nor that `--auto` never
  matters, since per its help it auto-approves whatever would otherwise be *asked*. `--agent plan`
  *declined* to write — model restraint, not a layer refusing it. All enforcement booleans are False.
- `--variant <level>` **is honoured, but silently ignored with no warning at all** on a value it
  doesn't recognise — worse than claude's stderr notice. Verified on
  `opencode/muse-spark-1.3-contributor-free`, one prompt, three paired runs per level: reasoning
  tokens rose monotonically with the variant — 75/95/99 at `minimal`, 172/201/230 at `low`,
  239/254/308 at `xhigh`. Non-overlapping, but note the low-to-xhigh margin is narrow (230 vs 239);
  the wide, unambiguous separation is `minimal` against `xhigh`, and `minimal` is deliberately
  outside Harflow's vocabulary. So the flag is directional, not a calibrated dial. Support is
  **per model**:
  `opencode models --verbose` lists each model's accepted `variants`; `muse-spark-1.2/1.3` accept
  `minimal, low, medium, high, xhigh`, `ling-3.0-flash-fin` accepts only `low, medium, high`, and
  `big-pickle`, `mimo-v2.5` and both `nemotron` models declare **no** `variants`, so `--variant` is a
  silent no-op on them. `max` — the value opencode's own `--help` gives as an example — appears in no
  model's variant list.

**vibe (Mistral Vibe CLI 2.25.1)**
- **Select the Python entrypoint for headless dispatch (2.26.1, 2026-10-24).** The installed
  `launcher.py` dispatches `VIBE_CLI=python` to the Python entrypoint and `VIBE_CLI=rust` to Rust.
  With no selector, it follows the cached GrowthBook `vibe_cli_rust_tui_rollout`; the default
  fallback is Python. Isolated no-paid-call `runpy` stubs showed the default Python path, forced
  `VIBE_CLI=python` bypassing the rollout, and Rust selection when the rollout was simulated as
  selected. An isolated temporary-home `VIBE_CLI=python vibe --help` exited 0, emitted no stderr
  and showed the Python `-p/--output` parser. This invocation selector is validated as an exact
  environment contribution and applied to headless start/resume, including both prompt transports.
  Interactive takeover inherits the user's environment. The issue reports the Rust TUI failure
  text: `You are using the new Vibe TUI, currently in preview… To switch back to the classic TUI,
  set VIBE_CLI=python.` That crash was reported, not reproduced in this check.
- `get_prompt_from_stdin()` (`vibe/cli/cli.py:54-67`) runs unconditionally before mode dispatch and
  calls `sys.stdin.read()` whenever stdin is **not a tty** — a pipe that never closes blocks forever.
  Small prompts use `stdin=DEVNULL`. Prompts over 32 KiB use `pipe_once` with trailing
  `--prompt=`: send UTF-8 bytes and immediately close stdin. Native stdin ingestion strips outer
  whitespace, so large prompts with outer whitespace are refused explicitly rather than changed.
- Programmatic mode is `-p/--prompt TEXT` with `--output streaming` for newline-delimited JSON.
- **The `PROMPT` positional is ignored in programmatic mode.** `cli.py:172` reads
  `args.prompt or stdin_prompt`; the positional serves interactive mode and worktree naming only
  (`entrypoint.py:233`). So there is **no working `--` separator** of the kind codex and opencode
  rely on — the prompt must ride on `--prompt`, and `-p` is `nargs="?", const=""`, so a prompt
  starting with `-` is not taken as its value (it falls through to stdin and dies with
  `Error: No prompt provided for programmatic mode`, exit 1 — loud, not silent, but it rules out the
  space-separated form for such prompts). Hence the single canonical token `--prompt=<text>`, last in
  argv for small prompts; the empty token selects stdin for large prompts. Measured: a prompt beginning with `--max-turns` reached the model verbatim at exit 0,
  unparsed as a flag.
- `sessionId` is a UUID vibe mints itself, present on **every** stream entry including the first, so
  the id is known from line one — `chooses_session_id=False`.
- **vibe renames its own process — measured 2026-09-26 on 2.25.8.** `vibe/cli/entrypoint.py:385`
  calls `setproctitle(process_name())` (`vibe/cli/_process_title.py`), so `ps -o command=` shows
  `Vibe CLI` and the argv markers recorded at spawn never match from then on. Until the fix this
  read as a false death: a *running* vibe task was listed `failed` by `ctl list`/the Monitor
  sidebar, and `resume` refused it, for as long as it ran. Status and the resume guards
  (`store.record_process_alive`) now use the recorded start time when one is present, so a
  retitled live run reads `running` again. Still open: cross-process cancel/takeover of a running
  vibe task stays refused — `may_signal` still requires the markers and reports `markers_missing`
  — which is a deliberate second factor, not an oversight.
- **No terminal event, no dollar cost, no token counts at all.** `--output streaming` emits only
  `PublicHistoryEntry` objects with `generation_status == COMPLETED`. Classification is
  exit-code-authoritative with the closing assistant message as corroboration — the same shape as
  codex, and codex only. opencode looks similar but is not: its `step_finish(reason="stop")` is a
  real end-of-run signal, so it can complete without an observed exit where codex and vibe cannot.
- **History replay on resume, measured.** A `--resume` run emitted, in order: the prior user message,
  the prior `reasoning`, the prior assistant message, a `checkpoint`, and only then the live turn.
  Replayed entries carry `turnId: null` and `source: "harness"`; the live turn's entries carry a real
  `turnId`, and its user message carries `source: "turn_start"`. Ingest must key off that
  `turn_start`/`turnId` marker, not off "first assistant message seen," or the replayed prefix leaks
  into the summary of a resumed task.
- **Turn-cap breach reads as failure, not a clean stop — unlike claude.** Hitting
  `--max-turns`/`--max-price`/`--max-tokens` raises `ProgrammaticLimitError` → exit 1. Measured with
  `--max-turns 1` on a prompt needing a tool call: exit **1**, stderr
  `<vibe_stop_event>Turn limit of 1 reached</vibe_stop_event>`. **The trap is in the stream, not just
  stderr:** the run also emits a *live-turn* `assistant` message whose text is that same
  `<vibe_stop_event>…</vibe_stop_event>` marker, so a naive ingest reports the marker itself as the
  agent's answer. `supports_turn_cap=True`, with that caveat attached.
- **A denial can end a run that already spoke — and a clean exit after one now reads `completed`
  with a warning.** The refusal above is not only a `publish` concern: at *any* freedom, a dispatch
  whose next command falls outside the user's `[tools.bash]` config is auto-denied and frequently
  stops there. Measured 2026-09-22 on two real dispatches replayed through `ingest`: one narrated
  its next step, had bash denied, **changed zero files**, and exited 0 — at the time reported
  `completed` with that narration as its `summary`, because `saw_final_message` had already latched
  on a message that preceded the refusal. So `ingest` **withdraws** the close when it records a
  denial, exactly as the stop-event branch does — but **without latching**, which is the whole
  distinction: a turn cap is terminal, a refusal is not, so a later assistant message in the same
  turn re-establishes the close. The denial stays on `acc.denials` either way.
  The withdrawal alone then made *every* run that ended on a refusal read `failed` — including a
  real dispatch whose every edit had landed and whose only sin was that its last action was refused
  before a clean exit. That is the opposite overclaim, so the policy changed (decided 2026-09-26 by
  the owner): a turn that ends on a refusal and then exits 0 reports **`completed` with a warning**
  naming the refused command, not `failed` with no summary. Every genuine failure signal still wins
  first — an error, a non-zero or unobserved exit, a turn-cap breach — and a run that recovers
  (answers after the refusal) completes with no warning at all. The accepted trade-off, stated so it
  isn't rediscovered as a bug: this reintroduces the 2026-09-22 false success. A run that narrates a
  step, is refused, changes nothing and exits 0 will read `completed`; the warning is then the only
  signal, and for an editing task the worktree is the check.
  **What this still does not fix**, and the limits are worth stating precisely. The second dispatch
  simply ended mid-work with no denial at all, and nothing in the stream distinguishes that from a
  short legitimate answer — it still reads `completed`. A denial followed by a further assistant
  message also re-establishes `completed`, and that message can itself be progress rather than an
  answer; vibe's stream offers no structural way to tell them apart, so neither can Harflow.
  **For an editing task, then, a vibe `completed` does not establish that the requested changes
  landed — check the resulting worktree and run the tests.** Not stated as "the diff is the only
  reliable signal", which overclaims in the other direction: a diff attributes nothing to a
  particular dispatch, and says nothing at all about a read-only or analysis task.
- **An `effect`'s `tool_result` is now normalized too, not silently dropped.** Every `effect` was
  already turned into a `tool_call`, but vibe carries no separate completion event for one — the
  same `id` is re-emitted as its `state.status` moves from a non-terminal value to a terminal one
  (`completed`/`failed`/`cancelled`, measured against real `~/.harflow/tasks` logs, e.g. a
  `file_edit failed`). `_normalize_effect` now tracks seen ids in two `stream_state` sets
  (`normalize_seen_effect_calls`, `normalize_seen_effect_results`) so the `tool_call` still fires
  exactly once and a `tool_result` fires exactly once, `ok = (status == "completed")`, output from
  `state.output` else `state.error`. An id already terminal the first time it is seen yields both
  events, call then result, in one pass.
- **`publish` on vibe needs `--agent auto-approve`, and so is no narrower than `unrestricted`.**
  Measured: under `--agent accept-edits`, `git commit -am wip` in a throwaway repo emitted a
  `callback` (`detail.kind: "approval"`, `title: "Allow bash?"`), was auto-denied, produced an
  `effect` with `state.status: "cancelled"`, and **no commit landed** — `accept-edits` auto-approves
  `write_file`/`edit` only, leaving `bash` governed by the user's own `[tools.bash]` allowlist, which
  did not cover `git commit`. `auto-approve` is the only profile that can publish, and it sets
  `bypass_tool_permissions: true`, removing every *other* restriction with it. So on vibe the
  `publish` level is a relabelling of `unrestricted`, and `Enforcement` says exactly that.
- **Effort is unsupported outright, and the reason is config precedence, not the missing flag alone.**
  vibe has no `--model` and no reasoning-effort flag — both are config-only
  (`[[models]].thinking`, vocabulary `off/low/medium/high/max`). Its layer precedence, quoted from
  `default_orchestrator.py:30-33`, is *lowest to highest*: schema defaults, GrowthBook experiments,
  **user TOML, project TOML, `VIBE_*` env vars**, runtime overrides, agent profile overrides, enforced
  admin config. A *trusted* project `.vibe/config.toml` (and Harflow passes `--trust`) outranks the
  user TOML and can repoint `active_model`, so an env override keyed off the user config would apply
  to the **wrong model and silently no-op** — the agent-profile layer (Harflow always passes
  `--agent`) and any org admin layer both outrank an env var too, and neither is inspectable from
  here. That silent-no-op failure mode is exactly what this repo exists to prevent, so `model` and
  `reasoning_effort` are both declared unsupported and raised loudly rather than passed through best-
  effort.

**antigravity (`agy` 1.2.14, measured 2026-10-01; real captures in `tests/fixtures/antigravity_*.jsonl`)**
- **`-p`/`--print` takes the prompt as its value** — unlike claude's boolean `-p`. `agy -p
  --output-format …` exits 2 with `-p took "--output-format" as its prompt`. A positional, with or
  without `--`, is refused outright (exit 2, `unexpected argument … Prompts are read only from
  -p/--print, -i/--prompt-interactive, or stdin`), so there is no separator at all: the prompt rides
  as the single canonical token `--print=<text>`, as on vibe. A prompt of
  `--dangerously-skip-permissions --mode plan. …` was delivered verbatim and the run stayed in
  `request-review`.
- The parser is strict: an unknown or mis-split token exits 2 with `flags provided but not defined`.
- `--output-format stream-json` events: `init` (carries `conversation_id` on line one, plus
  `init.model` — null when no `--model` — and `init.permission_mode`), `step_update` (one per step
  state change: `step_type` `user_input` / `agent_response` (with `text_delta` while `ACTIVE`, and
  `usage` when `DONE`) / `tool` (with `tool_name`, `tool_info.parameters`, then `tool_info.output`
  or `tool_info.error` on the terminal state)), and a terminal `result` with `status`
  (`SUCCESS`/`ERROR`), `response`, `num_turns`, `usage`, optional `error` and `denied_actions`.
  **No dollar cost.** Exit 0 on success, 1 on an `ERROR` result.
- **A tool step's `state` does not show a denial; `result.denied_actions` does.** In plan mode a
  `write_to_file` step read `DONE` though no file was written; only `denied_actions:
  [{"action":"write_file",…}]` said so. A denied command step does read `ERROR` with `permission
  check failed … user denied permission`.
- **A headless denial ends the run as a success.** The first permission prompt it cannot show is
  auto-denied, the agent stops, and the run reports `status: SUCCESS`, often an empty `response`,
  exit 0, with stderr `no output produced — a tool required the "<perm>" permission that headless
  mode cannot prompt for`. Harflow follows vibe's policy for this: completed, with a warning naming
  what was denied.
- Freedom, measured against a scratch repo with a local bare remote:
  - default (`request-review`) and `--mode plan`: workspace file writes denied; read-only commands
    (`git status`) run without approval; mutating commands (`touch`, `git commit`) denied. Plan mode
    also writes its own plan artifacts under `~/.gemini/antigravity-cli/brain/<id>/` — outside the
    repo, and allowed.
  - `--mode accept-edits`: workspace writes auto-approved; a write to a path outside the workspace
    was denied; mutating commands still denied.
  - `--dangerously-skip-permissions`: file write, `git commit`, `git push` to the remote, `curl`
    (HTTP 200) and a write outside the repo all succeeded.
  - All of the above is agy's own permission layer plus the user's `settings.json` allow-rules,
    which can widen it — nothing is OS-enforced.
  - `--sandbox` is OS-level, but only for **commands**, and it is not usable for real work. Commands
    could not write inside the repo, nor read `~/.gitconfig` (so every git command failed) or
    `~/.zshrc`, yet `curl` reached the network and `/tmp` was writable. The file-writing *tool* wrote
    outside the repo under it. Harflow maps no freedom to it. (Measured from inside Claude Code's
    own sandboxed shell, so the in-repo denial may partly reflect that nesting.)
- `--conversation <id>` resumes the same id (verified). **An unknown id is not refused:** stderr
  `warning: conversation "<id>" not found`, a *new* conversation with a different id, exit 0. So
  `ingest` must not let a resumed run's id silently replace the one asked for.
- No way to choose the id up front (`--conversation <fresh-uuid>` is just the not-found case), so
  `chooses_session_id=False`. `--add-dir <repo>` is accepted and harmless, which puts the repo on
  the command line as the identity marker for a fresh run. agy keeps its argv as its process title.
- `--effort low|medium|high|max`: `xhigh` is refused loudly (exit 1, `invalid --effort "xhigh"`).
  It works only with the default model. A model id that encodes its own level
  (`gemini-3.8-flash-low`) plus `--effort` exits 1 with `conflicts with --effort=high`, and
  `claude-sonnet-4-6` exits 1 with `--effort is not supported for model`. Base family names (`gemini-3.8-flash`,
  `gemini-3.1-pro`) do accept it. On the default model, three paired runs gave `low` 0/70/0
  thinking tokens and `high` 145/134/138: non-overlapping, so the levels change behaviour there.
- An unknown `--model` exits 1 with an `ERROR` result whose `conversation_id` is empty.
- **Live input: `--input-format stream-json`, one line per message:
  `{"event":"user","message":{"content":"<text>"}}`.** A claude-shaped line is refused (`missing the
  "event" field`), and so is a `--print` alongside it (exit 2).
  - The process idles after a `result` until more input or EOF, and EOF after a result exits 0 in
    ~0.2 s.
  - **Each written line is its own turn, even mid-turn** — unlike claude, which folds a mid-turn
    message into the current turn. Lines already written when stdin hits EOF still run, one turn
    each, before exit (measured: two queued lines, EOF at the first result, three results).
  - EOF mid-turn lets that turn finish, and resume takes the same live shape.
  - `result.usage` and `num_turns` are cumulative per process: the second result's usage is roughly
    twice the first's, and a resumed live run reported `num_turns: 3`.

**MCP client request deadlines (measured 2026-10-17 with `scripts/probe-mcp-client-deadlines.py`)**
A stub stdio MCP server whose tool holds the request open, driven through a localhost fake model
API, with isolated HOME/CLAUDE_CONFIG_DIR/CODEX_HOME and no model calls. Measured on these versions
only.
- **Claude Code 2.1.302** (`claude -p --strict-mcp-config`) sends a `progressToken` on `tools/call`.
  Default settings: holds of 70, 130 and 330 s returned normally, with progress every 5 s and
  without; **no deadline was observed up to 330 s** (longer not measured). With
  `MCP_TOOL_TIMEOUT=30000` the call fails at exactly 30 s, with or without progress, so progress
  does not extend that deadline; the server's handler got `CancelledError` at the deadline.
- **Codex codex-cli 0.163.0** (`codex exec`) sends a `progressToken`. MCP tools are reachable only
  through code-mode `exec` (`tools.mcp__<server>__<tool>`), which yields with "Script running with
  cell ID N" after ~30 s; the model must call `wait` on the cell (model behaviour, not a failure).
  Under `approval_policy="never"` the server entry needs `default_tools_approval_mode = "approve"`
  or the call is refused ("MCP tool call requires approval, but approval policy is never"); that is
  how the probe was configured, not something the installer sets. **Default `tool_timeout_sec` is
  300 s**: 70 and 130 s holds completed with and without progress; a 330 s hold with progress
  failed at exactly 300 s ("timed out awaiting tools/call after 300s"), so progress does not extend
  it. `tool_timeout_sec = 600` let a 330 s hold complete; `= 30` failed a 70 s hold at 30 s with and
  without progress; `= 90` failed a 130 s hold with progress at 90 s. **Codex sent no cancellation
  to the server at the deadline**: the request stayed open server-side until the client exited, so
  an abandoned Harflow wait keeps observing until its own timeout or the ceiling. That is
  harmless (waits are read-only) and is why `until="terminal"` has a server ceiling.
- Caller guidance: on Codex a wait over 300 s needs `tool_timeout_sec` raised on the Harflow
  server entry, otherwise repeat the wait with the same arguments or keep bounded waits at or
  below 270 s; on Claude Code `MCP_TOOL_TIMEOUT`, if set, bounds every wait. Harflow still sends
  progress every 5 s (cheap, other clients may honour it), but neither measured client extends its
  deadline on progress.

**GitHub Copilot CLI (1.0.89) — not supported yet, because nothing can be measured here.** On this
machine every `copilot -p` run fails with `Access denied by policy settings`, exit 1, and the stream
warns `Third-party MCP servers are disabled by your organization's Copilot policy`, so a Harflow
registered inside it would not load either. Both a backend and a client wait on an account whose
policy allows the CLI.

All five CLIs mishandle an unsupported reasoning effort differently, which is exactly why Harflow
validates against its own closed `EFFORTS` vocabulary before any of them see a value: claude degrades
silently with a stderr warning, opencode ignores it silently with no warning at all, codex alone fails
loudly — as a mid-run API `400` — agy refuses one before any model call (exit 1, `invalid --effort`), and vibe has no flag to mishandle at all: its equivalent
(`[[models]].thinking`) is config-only, several layers removed from anything on the command line, and
an unsupported value there would surface (if it ever did) as a config-resolution problem, not a CLI
error. Because that vocabulary is only four literals (`low`/`medium`/`high`/`xhigh`), none containing
a quote or `=`, TOML-quoting codex's `-c model_reasoning_effort="<v>"` value is a non-issue — no
encoder needed, just the literal interpolated between quotes.

**Registering with them as MCP clients (`mcp add`, measured 2026-08-12; vibe measured 2026-09-10; agy 2026-10-01)**
- All four accept `--` and run headless with stdin closed. `stdin=DEVNULL` still matters:
  `opencode mcp add` is prompt-capable, and so is `vibe mcp add`.
- **Only Claude Code refuses to overwrite.** Re-adding exits **1** with
  `MCP server <name> already exists in user config` and writes nothing, and there is no
  `--force`/`--replace` on `add` or `add-json`. So an update means `remove` first — which is why a
  failed replacement must tell the user to *assume* nothing is registered (a failed `add` is not proof
  it wrote nothing) and print the restoring command.
- `claude mcp remove` also exits **1** for a name that was absent
  (`No MCP server named "<name>" in user scope`). Tolerate that signature and *only* that one:
  reading every exit 1 as "wasn't there" turns a permission error into licence to delete.
- **Match those signatures whole, with the name delimited.** `<name> already exists` also matches
  `MCP server not-harflow already exists…`, so a *different* server's collision would make us
  delete ours; the leading `mcp server ` and the quotes around the name in the not-found form are what
  make the checks safe.
- `claude mcp get` is not a readback. It prints human prose, not JSON, and **launches the server** to
  health-check it.
- Codex and opencode `mcp add` **overwrite**, and each preserved comments elsewhere in its own config
  (`config.toml`, `opencode.jsonc`). That is a property of the versions measured, not a promise.
- `codex mcp add` refuses an explicit `CODEX_HOME` that does not exist, but **creates** its default
  `~/.codex`. Only a sandboxed test needs to make the directory first.
- Run these from `$HOME`: a project-local `opencode.jsonc` in whatever directory the installer was
  run from would otherwise capture a registration meant to be global.
- **vibe is a fourth, distinct behaviour — it neither overwrites (codex, opencode) nor refuses
  outright (Claude Code). Its collision has _two_ shapes, and only one of them exits 0.** Measured
  against a sandboxed `VIBE_HOME`:
  - `vibe mcp add <new>` → `` Added MCP server `<name>`. ``, exit 0.
  - `vibe mcp add` with a **byte-for-byte identical** entry → `` MCP server `<name>` is already
    configured. ``, **exit 0, nothing written** — an idempotent no-op.
  - `vibe mcp add` with the **same name but different settings** — i.e. what an update actually is —
    → an argparse failure: a `usage:` block plus
    `` vibe mcp: error: MCP server name `<name>` is already configured. ``, **exit 2, nothing
    written**. Note the extra word **name**, which is what keeps the two signatures distinguishable.
  - `vibe mcp remove <existing>` → `` Removed MCP server `<name>`. ``, exit 0.
  - `vibe mcp remove <absent>` → `` MCP server `<name>` is not configured in the user config. ``,
    **exit 0** — unlike `claude mcp remove`, which exits 1 for the same case.
  - remove-then-add **does** update correctly (verified: the stored command changed). There is no
    `--force`/`--replace` on `add`, so that is the only way to replace an existing entry, and both
    collision signatures must route to it.
  - **A cautionary tale about measuring the easy case.** The first measurement here re-added an
    *identical* entry, saw exit 0, and recorded "silently skips at exit 0" as the general rule. The
    conflicting case — the only one an update ever hits — exits 2, so a client that treated any
    non-zero exit as fatal turned every update into a hard failure while leaving the stale entry in
    place. A real `HARFLOW_CLI_INTEGRATION=1` run against the binary is what caught it, not the unit
    tests written from the wrong fact. When measuring an idempotent-looking command, vary the
    payload, not just repeat the call.
  - Because every case *except* that conflict exits 0, **the exit code alone cannot tell success
    from a no-op for vibe** — the stdout message is the signal, and the exit code only narrows which
    message to expect. Match it whole, with the name delimited by its backticks, per the narrow
    signature rule above.
  - `vibe mcp add` also **rewrites the whole `config.toml` and destroys hand-written comments in it**
    (measured: a `# hand-written comment` was gone afterwards, and `args` was reformatted) — unlike
    codex's and opencode's own `add`, which preserved comments elsewhere in their configs.
- **agy** (sandboxed with a temp `HOME`; its config is `~/.gemini/config/mcp_config.json`, plain
  JSON `{"mcpServers": {"<name>": {"command", "args"?, "env", "disabled"}}}`):
  - **Flags must come before the name** (its own help says so), so the shared `add_argv` shape does
    not fit.
  - `agy mcp add --env K=V <name> -- <cmd> [args]` → `Added MCP server "<name>" (stdio)`, exit 0.
  - **It overwrites like codex.** Re-adding an identical entry and re-adding with different settings
    both exit 0 with the same message, and the stored entry is replaced.
  - `agy mcp remove <existing>` → `Removed MCP server "<name>"`, exit 0.
  - `agy mcp remove <absent>` → `Error: MCP server "<name>" not found`, **exit 1**, as with claude.
  - `agy mcp list` prints a human table, not JSON, so inspect reads the JSON file instead.



### Claude transcript display recovery (2.1.309, 2026-10-23)

Bounded local inspection without a model call confirmed exact-session files
under the configured Claude `projects/<directory>/<session-id>.jsonl` layout.
Records carry `sessionId`, `timestamp`, `uuid`, `type` and `message.content`.
Assistant text blocks carry `type: text` and string `text`; assistant tool_use
blocks carry string `id`/`name` and object `input`. User tool_result blocks carry
string `tool_use_id`, string or array `content`, and optional boolean `is_error`.
Field/type qualification inspected 64 bounded existing tails without displaying
or copying transcript content. Unsupported or malformed blocks are omitted.
This is display-only provenance, never a substitute for captured run outcomes.

Claude live dispatch context uses a second text content block in the existing
single stream-json user message, preserving the first caller text block exactly.
Its strict validator accepts only the generated bounded runtime note. Other
backends do not gain unqualified input formats or background tracking claims.

The second runtime text block was verified against installed Claude Code 2.1.309
using an isolated temporary HOME/config, fake local key and localhost fake
Anthropic API: one request, exit 0, original caller text block preserved exactly,
and generated runtime context visible in the request. No model service was used.

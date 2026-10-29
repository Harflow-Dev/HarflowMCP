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
  So the `publish` freedom drops the git denies *and* adds the allow entries; passing both there would
  be self-defeating, since deny beats allow. `--allowedTools` is variadic exactly like
  `--disallowedTools`, so its patterns are likewise one comma-separated value. `write_in_repo` with
  `allowed_commands` deliberately passes both (see "Claude allowed_commands" below): the deny wins.
  The merge-conclusion carve-out follows that measured fact rather than a new live Claude run:
  `git commit --no-edit` cannot be granted beside `Bash(git commit:*)`, so that deny entry is omitted
  only while the exact grant is present, and `Bash(git push:*)` stays. Other commit forms stay
  ungranted. The `MERGE_HEAD` check is Harflow's PATH wrapper, not a Claude CLI measurement.
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

**vibe 3.0.1 (requalified 2026-10-26, isolated HOME/VIBE_HOME and loopback fake provider)**
- Installed Python launcher unconditionally execs the bundled Rust CLI; `VIBE_CLI=python` is
  ignored. Harflow retains that harmless selector for older launchers, without claiming all older
  releases support the current profile. `plan` is removed; `read_only` now selects `ask`.
- `ask` is an approval policy, not a read-only write-tool exclusion. Inherited user/project
  policies or a custom ask profile can pre-approve writes. No OS boundary or confinement is claimed.
- Actual streamed start, explicit session resume and 40,005-byte one-shot stdin all exit 0;
  `sessionId`, `turnId`, `source: turn_start` and assistant text are compatible with the parser.
  The measured resume emitted live entries only; the parser still excludes historical replay.
- A scripted Bash mutation needing approval emits a matching-turn approval callback and failed
  `file_system.bash` effect; no file is created. The existing denial parser accepts that shape.
  A later scripted answer exits 0 and classifies completed with the denial retained. Setting
  inherited `[tools.bash] permission="always"` allows that same mutation under `ask`, measured.
- A one-turn cap after a tool call exits 3 with stderr `Stopped: turn_limit`, without an assistant
  stop envelope. Nonzero-exit classification already reports failed; historical stop-envelope
  handling remains for older streams. Vibe 3 caps are not automatically continued: only the
  older structured stop-envelope signal is certified for continuation. A provider usage-limit
  response (HTTP 402, fake `usage_limit` body, retry budget 0.1 seconds) is also a failed run
  and never automatically continued. This does not qualify HTTP 429 retry timing.
- Reproducible no-paid qualification: `HARFLOW_CLI_INTEGRATION=1 uv run pytest tests/test_vibe_cli.py`.
  Provider traffic is scripted on localhost with a custom local model and isolated configuration;
  this does not qualify paid-provider authentication, model reasoning or arbitrary user profiles.

**Historical vibe (Mistral Vibe CLI 2.25.1; superseded where stated above)**

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

### Claude refusal texts (2.1.310, 2026-10-24)

Measured with `claude -p --output-format stream-json --permission-mode acceptEdits` against a localhost
fake Anthropic API, an isolated `HOME`/`CLAUDE_CONFIG_DIR` and no model calls. Each refusal is an
`is_error` `tool_result` whose `tool_use_id` also appears in the result event's `permission_denials`.
The `system`/`init` event carries `claude_code_version`.

| Case | Exact `tool_result` text |
| --- | --- |
| deny rule (`--disallowedTools Bash(git commit:*)`; deny beats a matching allow) | `Permission to use Bash with command git commit -m x has been denied.` |
| unlisted command (`python3 -c ...`, `make build`, `git -C . commit`, `bash -c '...'`) | `This command requires approval` |
| chained command, one unlisted part (`;`, `&&`, `|`) | `This Bash command contains multiple operations. The following part requires approval: python3 -c 'print(1)'` |
| command substitution | `A nested command in this command can't be checked before it runs` |
| Read under a directory name ending in a space, even with `--allowedTools Read(**)` | `Claude requested permissions to read from /some/repo/trailing /file.txt, which contains a suspicious Windows path pattern that requires manual approval.` |
| Write outside the working directory | `Claude requested permissions to write to /some/out/x.txt, but you haven't granted it yet.` |
| caller rejection through the stdio permission transport (Harflow's own text, `claude_approvals.encode`) | `Caller rejected this exact action` |
| Harflow system refusal (Harflow's own text; not emitted yet) | `Harflow refused this action: <reason>` |

Harflow classifies only these texts, matched in full. "Contains brace with quote character" was
not re-measured, so it is detected as a refusal but stays unclassified. Re-capture with
`HARFLOW_CLI_INTEGRATION=1 uv run pytest -m cli_integration tests/test_claude_denials_cli.py`; on an install that is not in
the measured set the test skips unless `HARFLOW_QUALIFY_CLAUDE_VERSION` names the exact installed version.

### Claude `allowed_commands` (2.1.310, 2026-10-24)

Measured with `claude -p --output-format stream-json --permission-mode acceptEdits` against a localhost fake
Anthropic API, an isolated `HOME`/`CLAUDE_CONFIG_DIR` and no model calls. Re-run with
`tests/test_claude_commands_cli.py`.

- An unlisted command (`python3 -m calendar`, `make build`) is refused with `This command requires approval`; the
  same command listed in `--allowedTools` as `Bash(<command>)` runs with an empty `permission_denials`.
- `Bash(python3 -m pytest:*)` and `Bash(python3 -m pytest *)` both match `python3 -m pytest`, with or without
  arguments. Neither matches `python3 -m pytestx`: both respect the word boundary. Harflow accepts both spellings
  and documents `:*`.
- A chained command is allowed only when every part is allowed; otherwise the whole command is refused with
  `The following part requires approval: <part>`, naming the first unlisted part.
- A deny rule beats a matching allow rule: with `--disallowedTools Bash(git commit:*)` and
  `--allowedTools Bash(git commit:*)` together, `git commit -m x` is refused with `Permission to use Bash with command
  git commit -m x has been denied.` and recorded in `permission_denials`. This is why `write_in_repo` passes both
  lists, and why a pattern cannot re-enable a direct `git commit` command string there; a granted program, or code it runs, can still invoke git.
- The freedom's own entries and the extra patterns work in one comma-separated `--allowedTools` value, and also
  when split across two `--allowedTools` options. Harflow always writes one value, freedom entries first.
- Read-only commands (`uname -a`, `git status`, chains of them) and in-cwd filesystem commands (`touch`) are
  auto-approved by 2.1.310 `acceptEdits` and need no grant. Git preparation grants were added separately below.

### Claude caller approvals and Read/Write/Edit prompts (2.1.310, 2026-10-24)

Measured with `--input-format stream-json --permission-prompt-tool stdio --permission-prompts host` and
`--permission-mode acceptEdits`, against a localhost fake Anthropic API with an isolated `HOME` and
`CLAUDE_CONFIG_DIR`; no model calls. Re-run with
`tests/test_claude_caller_approvals_cli.py`, `tests/test_claude_approval_resume_cli.py` and
`tests/test_claude_write_approvals_cli.py`.

- A `can_use_tool` `control_request` carries `subtype`, `tool_name`, `input`, `tool_use_id`, `description`,
  `display_name`, `decision_reason`, `decision_reason_type` and `permission_suggestions`.
- Bash approve runs the command; reject records the denial with `Caller rejected this exact action`. A chained
  command produces one request for the whole command (`decision_reason_type: subcommandResults`); command
  substitution produces a request whose reason is the nested-command text. Deny-rule commands are never offered.
- A Read under a directory name ending in a space is emitted as a request (`tool_name: Read`,
  `decision_reason_type: other`, reason "Path contains suspicious Windows-specific patterns ... require manual
  verification", no suggestions); approve performs the read, reject records a denial.
- A Read, Write or Edit outside the working directory is emitted with `decision_reason_type: workingDir` and
  reason "Path is outside allowed working directories". A Write or Edit under a path-safety directory is emitted
  with `decision_reason_type: safetyCheck` and reason "Claude requested permissions to write to <dir>".
- Edit input keys are `file_path`, `old_string`, `new_string` and `replace_all`. Claude requires a prior Read, and
  that Read prompts too when the file is outside the working directory.
- Approving a Write or Edit performs exactly that write or edit; rejecting records a denial. Approving one Write
  does not grant the directory: a second Write to the same outside directory prompted again. Harflow returns no
  `updatedPermissions`, so the `addDirectories` and `addRules` suggestions are ignored.
- A Write or Edit answered with `Harflow refused this action: <reason>` is a denial like any other: the run
  continues and nothing is written.
- The `system`/`init` event's `claude_code_version` equals `claude --version`; Harflow cross-checks the two.

### Codex app-server (codex-cli 0.163.0, 2026-10-25)

Summary: Codex caller approvals are implemented through `codex app-server` and qualified only for codex-cli 0.163.0
on macOS (`backends/codex_qualification.py`, the single table); [harness permissions](harness-permissions.md#codex-caller-approvals-codex-app-server)
describes the behavior. The measurements below are what the design depends on; a version bump must re-measure each
fact recorded in the table row.

Measured with `codex app-server --listen stdio://` over JSON-RPC, against a localhost fake Responses API
(`tests/fake_responses_api.py`), an isolated `HOME` and `CODEX_HOME`, and no model calls. The trimmed protocol schema is
`tests/fixtures/codex_app_server/schema-0.163.0.json` (generated with `--experimental`; an opt-in test compares it with
the installed Codex). The driver and scenario scripts are kept outside this repository. Recorded frames: only the
transcripts the tests use are in `tests/fixtures/codex_app_server/transcripts/` (`index.json`, `in_repo: true`:
`a1-basic`, `a2-on-request-workspace-write-cancel`, `f1-response-failed`, `f1-http-400`, `p0c-r6-a-start`,
`p0c-r6-b-resume`). Every other transcript cited below is **archived** outside the repository and listed in
`index.json` under `archived` with `in_repo: false`; the rows they back are measurements, not reproducible from the
repository alone. Unless a row says otherwise, thread settings were `approvalPolicy: on-request`,
`approvalsReviewer: user` and `sandbox: workspace-write`.

**Lifecycle**
- `initialize` (params `clientInfo`, optional `capabilities.experimentalApi`) answers `userAgent`
  (`<clientInfo.name>/0.163.0 (<os>) <terminal> (<name>; <version>)`), `codexHome`, `platformFamily`, `platformOs`.
  `thread.cliVersion` in the `thread/start` response is also `0.163.0`. A request before `initialize` is
  JSON-RPC error -32600 "Not initialized"; a second `initialize` is "Already initialized".
- Unknown params fields are ignored; experimental fields need `experimentalApi` (`experimentalRawEvents` without it is
  -32600). `availableDecisions` is **not** gated: it is present on command requests without `experimentalApi`, but
  absent from the stable-schema bundle.
- `thread/start` answers `approvalPolicy`, `approvalsReviewer`, `sandbox` (`workspaceWrite`, `readOnly`,
  `dangerFullAccess`), `activePermissionProfile`, `cwd`, `model`. `turn/start` answers `turn.status: inProgress`;
  `turn/completed` carries `status` `completed`, `interrupted` or `failed` (with `error`). A model-endpoint failure
  produces `error` notifications (`willRetry: true`, "Reconnecting... n/5") and, after the retries, `turn/completed`
  `failed`; an HTTP 400 fails at once with `willRetry: false`.
- Stdin EOF, idle or with a request held: exit 0 within about 50 ms, the held command never runs. SIGTERM while held: exit 0.
  SIGKILL of the driver while held: the wrapper and the native child both exited within 2.5 s. `codex app-server` is a
  node wrapper whose native child shares the wrapper's stdin/stdout/stderr pipes; tool processes are children of the
  native process.
- Approval requests are preceded by `thread/status/changed` with `activeFlags: ["waitingOnApproval"]`. After the answer,
  `serverRequest/resolved` (`threadId`, `requestId` as the original JSON-RPC id type) arrives, then the status returns to
  empty flags. `turn/interrupt` while a request is held answers `{}`, then emits `thread/status/changed idle`,
  `turn/completed interrupted` and only then `serverRequest/resolved`; a late `accept` is ignored, nothing runs, and the
  thread accepts a new `turn/start`.

**Command approvals (`item/commandExecution/requestApproval`)**
- Params seen: `kind`, `threadId`, `turnId`, `itemId`, `startedAtMs`, `environmentId`, `command`, `cwd`, `commandActions`,
  and when applicable `reason`, `proposedExecpolicyAmendment`, `availableDecisions`, `additionalPermissions`, `approvalId`.
  Optional keys are **absent**, not null. `networkApprovalContext` and `proposedNetworkPolicyAmendments` never appeared.
  `command` is the shell wrapper (`/bin/zsh -lc '<cmd>'`); `reason` is the model's justification, or for a
  `prompt` rule `` `<command>` requires approval: <justification> ``.
- `itemId` is the model call id, or `exec-<uuid>` for the nested `exec_command` of the code-mode `exec` tool. The
  `item/started` commandExecution item precedes the request, with the same `command`.
- `availableDecisions` is `["accept", {"acceptWithExecpolicyAmendment": {...}}, "cancel"]` when a proposed amendment
  exists, else `["accept", "cancel"]` (prompt rules, `with_additional_permissions`, `writeStdin`). **It never contains
  `decline`**, yet `decline` is accepted and means "deny, continue the turn".
- `accept`: the item completes with the command's output. `decline`: item `status: declined`, the model sees
  `exec_command failed: CreateProcess { message: "Rejected(\"rejected by user\")" }`, the turn continues. `cancel`: the
  item is `declined` and the turn ends `interrupted`. A JSON-RPC error answer: item `status: failed`, model text
  `Rejected("approval request failed")`, the turn continues. A malformed decision (`{}`, unknown string, wrong variant)
  fails deserialization (stderr), item `failed`; for a file change it is a decline.
- `acceptForSession` auto-approved an identical later command without a request (session cache).
  `acceptWithExecpolicyAmendment` wrote `rules/default.rules` in `CODEX_HOME` and later matching commands then ran
  with no request. Neither may be sent.
- **A command request does not say whether accepting it leaves the sandbox.** Accepting a model `require_escalated`
  request ran the command unsandboxed (wrote outside the workspace), including when a `prompt` rule also matched.
  Under `approvalPolicy: untrusted`, accepting an ordinary command also ran unsandboxed. Accepting a request raised only
  by a `prompt` rule (no escalation requested) ran the command inside the sandbox (the outside write failed). The params
  of these cases are the same shape (`kind: command`, `availableDecisions [accept, cancel]` for the prompt cases).
- With `on-request`, a plain command that fails in the sandbox (`Operation not permitted`) neither asks nor retries;
  the model sees the shell error and **no item is emitted** on the app-server stream. A command denied by an exec-policy
  `forbidden` rule emits no item either: the model sees `Rejected("`<command>` rejected: <justification>")` and the same
  text is on stderr. `approvalPolicy: never` with a `require_escalated` call: model text `approval policy is Never;
  reject command — you cannot ask for escalated permissions if the approval policy is Never`.
- Multiple approvals per `itemId` occur: a `write_stdin` to a still-running escalated terminal raised a second request
  with the same `itemId`, `kind: "writeStdin"`, `approvalId` (the `write_stdin` call id), `command`
  `write_stdin --session-id <n> '<chars>'`, `availableDecisions ["accept", "cancel"]`. Under `untrusted` the same
  write did not ask. A compound command (`a && b`) is one request; `proposedExecpolicyAmendment` covers only the first
  program. The zsh-fork feature is removed in this version.
- The `exec` code-mode tool is used by the `gpt-6.1-sol` model slug; `gpt-5` and unknown slugs get direct
  `exec_command`, `gpt-5.5` also a freeform `apply_patch`. A declined nested call raises a script error in the `exec`
  program.

**File changes (`item/fileChange/requestApproval`)**
- Params: `threadId`, `turnId`, `itemId`, `startedAtMs`, `reason` (null), `grantRoot` (always null in every case
  measured). No `availableDecisions`, no paths: join with the preceding `item/started` fileChange item by `itemId`
  (`changes[]` of `path`, `kind`, `diff`). Order: `item/started`, `thread/status/changed`, request, answer,
  `serverRequest/resolved`, `item/completed`. `changes` in started and completed were identical in every case.
- A move is `kind: {"type": "update", "move_path": <destination>}` with `path` as the source; the diff text ends with
  `Moved to: <destination>`. Delete is `{"type": "delete"}`. One patch with several files is one request.
- Requests appear for patches that write outside the workspace root (`workspace-write`), for any patch under
  `read-only`, and for any patch under `untrusted`; in-workspace patches under `workspace-write` do not ask.
  `accept` does not grant the root (a later outside patch asked again). `decline`: `item/completed` `status: declined`,
  model text `patch rejected by user`. `cancel`: the turn is `interrupted` and the started item is never completed.

**Other server requests**
- `item/tool/requestUserInput` only in plan collaboration mode (set by `turn/start`); in default mode the tool answers
  "request_user_input is unavailable in Default mode" and no request is sent. A JSON-RPC error yields `{"answers":{}}`.
- `item/permissions/requestApproval` only with `features.request_permissions_tool` (and `exec_permission_approvals` for
  `with_additional_permissions` commands), both off by default but enableable from config. Its response has no decision
  field: `{permissions, scope}`. Granting `permissions` let later commands write the granted path without asking; `{}`
  grants nothing; a JSON-RPC error or a wrong-shape response (`{"decision":"decline"}`) also grants nothing. A command
  using `with_additional_permissions` raises a command request carrying `additionalPermissions` and
  `availableDecisions ["accept", "cancel"]`.
- MCP tool calls ask through `mcpServer/elicitation/request` (`_meta.codex_approval_kind: mcp_tool_call`, `serverName`,
  `message`, `_meta.tool_params`); it has **no `itemId`** and arrives before the `mcpToolCall` item starts. Response
  `{"action": "accept"|"decline"|"cancel", "content": null}`; a JSON-RPC error is a rejection ("user rejected MCP tool
  call"). MCP tools are namespaced `mcp__<server>` in the model tool list.
- `applyPatchApproval`, `execCommandApproval`, `item/tool/call`, `account/chatgptAuthTokens/refresh` and
  `attestation/generate` were never emitted by the v2 flows exercised.

**Configuration precedence and mid-run reload**
- Precedence for the thread's approval policy, reviewer and sandbox: explicit `thread/start`/`thread/resume` params win
  over the `thread/start` `config` map, which wins over argv `-c`, which wins over a project `.codex/config.toml`
  (trusted projects only), which wins over user `config.toml`. All of project config, user config, argv `-c` and the `config`
  map lost to explicit params in every adversarial case (`never`, `danger-full-access`, `auto_review`). The thread
  response reports the effective values for each field, including for unpinned threads.
- `turn/start` accepts `approvalPolicy`, `approvalsReviewer`, `sandboxPolicy` and `permissions`; hostile values there
  override the thread pins for the turn without being visible in the `thread/start` response.
- Unpinned `thread/resume` in a new process keeps the thread's stored `approvalPolicy` and reviewer but takes the
  **sandbox from current config**. Pinned resume applies the pins. Resuming a thread already loaded in the same
  process ignores new pins.
- `thread/start` with `workspace-write` on an untrusted git repository adds `[projects."<repo>"] trust_level =
  "trusted"` to `CODEX_HOME/config.toml`, after which project config, rules and hooks of that repository are live. An
  untrusted project is otherwise ignored (unpinned default is `on-request` with `read-only`).
- Project `.codex/config.toml` is read from the git root down to `cwd`; `.codex` above the git root and `$HOME/.codex`
  are not read. A legacy `profile = "<name>"` key in user config makes `thread/start` fail (-32600); `--profile` exists
  on `codex`, not on `codex app-server`.
- Exec-policy rules load from `CODEX_HOME/rules` and a trusted project's `.codex/rules`. `allow` rules run matching
  commands without a request and **unsandboxed**, for model-requested escalation and for plain commands alike;
  `forbidden` rules deny; `prompt` rules raise a request. None of this is visible in the thread response.
- Hooks (`[[hooks.PermissionRequest]]` in a config.toml, or `hooks.json`, at user or project level) are discovered but
  `untrusted` until `[hooks.state."<key>"] trusted_hash` is present in user config (`hooks/list` returns `source`, `key`,
  `currentHash`, `trustStatus`). A trusted `PermissionRequest` hook returning `allow` resolved both command and file-change
  approvals: the client got **no request**, the commands ran, and the only signals were `hook/started` and
  `hook/completed` notifications and `hooks/list`. `-c features.hooks=false` (argv) and the `config` map
  `{"features": {"hooks": false}}` disabled even a trusted hook.
- `approvals_reviewer = "auto_review"` in config, with the reviewer not pinned, is reported in the thread response; the
  reviewer then answers escalations itself (`item/autoApprovalReview/*`, `guardianWarning`) with extra model requests
  and no client request. Pinning `approvalsReviewer: user` overrode it; `features.guardian_approval` did not change a
  pinned thread.
- Sandbox write protection under `workspace-write`: `.git`, `.codex` and `.agents` are not writable at the git root and at
  `cwd`; `.codex` in a deeper subdirectory is writable (and is read when a thread later starts with that `cwd`).
  `CODEX_HOME` is writable only when it lies under a writable root (`$TMPDIR`, `/tmp`, the workspace); with a
  `CODEX_HOME` outside those, writes to `config.toml`, `rules/` and new files failed.
- No reload: after an approved command rewrote `.codex/rules`, `.codex/config.toml` and `CODEX_HOME/config.toml`, the next
  command in the same turn, the next turn in the same process and an in-process resume behaved as before. A resume in a
  **new process** applied the rewritten rules (the rule-allowed commands ran without asking). A syntactically broken
  user config makes `thread/resume` fail with a load error.

**Pipe isolation (macOS)**
- Tool processes (sandboxed and approved-escalated, including a live session running while another request was held) have
  fd 0 on `/dev/null` and fds 1/2 on pipes whose peer ends belong to the native codex process, none of which are the
  wrapper's or native process's fds 0/1/2. `lsof -p <app-server pid>` works from inside the sandbox and lists the
  app-server's pipe objects, but macOS has no path to open another process's descriptors. Writes by a tool to its own
  fds appeared only as `item/commandExecution/outputDelta` text; injected response and request frames were never answered
  or applied, and the held request was unaffected. `ps` is denied in the sandbox.
- Background processes (`&`, `nohup`, `setsid`, double fork) were all killed when the `exec_command` call ended, sandboxed
  or escalated; only a command that yields a session id (long-running) stays alive between calls.
- Not exercised: debugger attachment or `task_for_pid` by an approved unsandboxed command against the codex process.

**Interop**
- `thread/resume` of a thread created by `codex exec`, and `codex exec resume` of a thread created by app-server, both
  worked and carried the earlier history. `thread.started.thread_id` from exec equals the app-server thread id.

**Follow-up measurements (same setup; archived transcripts `h2-*`, `s3-*`, `cfg-w4-*`, `cfg-fp-*`, `x6-*`, `t8*`)**
- Hooks under pins: with `-c features.hooks=false`, `hooks/list` returns **no hooks** (there is no `enabled: false`
  entry) and nothing fires. With only the `thread/start` `config` map `{"features":{"hooks":false}}`, `hooks/list` (which
  takes no config map) still reports the hook as `enabled: true, trustStatus: trusted`, but the hook does not fire on
  that thread. So `hooks/list` proves the argv pin, not the map pin; both should be sent and the absence of `hook/*`
  notifications checked.
- A trusted `PreToolUse` hook can only **block**: `permissionDecision: deny` blocks the command (no client request, no
  item, model text `Command blocked by PreToolUse hook: <reason>`, `hook/completed` status `blocked`); `allow` is
  rejected (`hook/completed` status `failed`, entry "PreToolUse hook returned unsupported permissionDecision:allow") and the
  request still reaches the client. Only `PermissionRequest` hooks approved on their own. With `features.hooks=false` neither fires.
- Notification methods that signal a resolution not made by the client: `hook/started` and `hook/completed`
  (`run.eventName`, `run.source` user/project, `run.status` completed/failed/blocked), `item/autoApprovalReview/started` and
  `item/autoApprovalReview/completed` (`targetItemId`, `decisionSource`, `review.status`), `autoApprovalReview/strictReviewRequired`
  (never observed) and `guardianWarning`. `serverRequest/resolved` is emitted for client answers and is not such a signal.
  An exec-policy `allow` rule produces **no notification at all**. `warning`, `configWarning` and `error` are unrelated.
- Live session survival: a long-running command that yields a session id (escalated or sandboxed) runs in its **own
  process group** (pgid equals its shell pid; `ps` `sess` prints 0 on macOS), a child of the native codex process, while the
  wrapper and native share the driver's group (or the new group when spawned with `start_new_session`). Stdin EOF, SIGTERM
  of the wrapper and `killpg(SIGTERM)` of the app-server group all end the native process and every session. **SIGKILL of
  the wrapper alone leaves the native process (ppid 1) running with its pending request and all sessions; `killpg(SIGKILL)`
  of the app-server group kills the wrapper and native but leaves every tool process group running (ppid 1).**
  A terminate-then-wait with SIGTERM to the group is the clean path; after SIGKILL, tool descendants must be found separately.
- Ambient sandbox widening: a trusted project config with `[sandbox_workspace_write] writable_roots=[<dir>]`,
  `network_access=true`, `[shell_environment_policy] inherit="all", set={...}` and an extra `[mcp_servers.x]` took effect
  in full under explicit `approvalPolicy`/`approvalsReviewer`/`sandbox: workspace-write` pins (outside write succeeded,
  localhost connect succeeded, MCP server started). The `thread/start` response **does** expose it:
  `sandbox.writableRoots`, `sandbox.networkAccess`, `runtimeWorkspaceRoots` (and `activePermissionProfile`). Pinning
  `sandbox_workspace_write.writable_roots=[]` and `.network_access=false` through the `config` map or through argv `-c`
  overrode the project values (confirmed by sandboxed write and connect failures and by the response). Maps merge,
  though: `shell_environment_policy.inherit="core"` took effect, but `set={}` did not remove a project `set` entry, and
  `mcp_servers={}` did not remove a project MCP server; a named entry is overridable
  (`mcp_servers.<name>.enabled=false`, `shell_environment_policy.set.<VAR>="..."`). Neither is shown in the thread
  response; MCP servers appear as `mcpServer/startupStatus/updated` notifications.
- Feature pins: `-c features.request_permissions_tool=false` / `features.exec_permission_approvals=false` and the same keys in
  the `config` map override a config that enables them (the `request_permissions` tool and `exec_command`'s
  `additional_permissions`/`with_additional_permissions` disappear from the model tool list). An unknown `features.*` key is
  silently ignored.
- Cross-transport resume with pins: a thread started by `codex exec` with `never` and `danger-full-access`, resumed by app-server
  with explicit pins, reports the pinned `on-request`/`workspaceWrite`/`user`; an unpinned resume reports
  config defaults rather than the exec thread's original values.
- Same-process resume of an already loaded thread ignores new pins and returns the existing settings (`r1-resume-a`).
- Trust: stored in `CODEX_HOME/config.toml` as `[projects."<git root realpath>"] trust_level = "trusted"`, keyed by the git
  root even when `cwd` is a nested directory. `config/read` returns it under `config.projects` without mutating anything
  (absent before the first workspace-write start). `thread/start` with workspace-write on an untrusted repo writes the entry;
  read-only does not. An explicit `untrusted` entry is kept (workspace-write pin still honoured, project config ignored).
  Passing `{"projects": {"<root>": {"trust_level": "trusted"}}}` in the `thread/start` `config` map makes project config
  effective **without writing config.toml**, and `"untrusted"` there ignores it; argv `-c projects."<root>".trust_level=...`
  still writes the file. The current-shape `codex exec --json -s workspace-write -c approval_policy="never" -C <untrusted repo>`
  also writes the trust entry; `-s read-only` does not.

**Trusted hostile project under R6 pins (transcripts `p0c-r6-a-start` and `p0c-r6-b-resume` in the repository; the control runs `p0c-control-*` are archived; index ids `0c-*`)**
- Setup: `CODEX_HOME/config.toml` marks the throwaway git root `trust_level = "trusted"` and trusts a project
  `PermissionRequest` hook; the repo's `.codex/rules` allows `touch`, `printenv`, `curl`, `sh`, `zsh`, `/bin/zsh` and `echo`;
  its `.codex/config.toml` sets `approval_policy = "never"`, `danger-full-access`, `approvals_reviewer = "auto_review"`,
  `writable_roots = [<outside dir>]`, `network_access = true`, `shell_environment_policy.set = {HOSTILE_VAR}` and an MCP
  server that creates a marker file when launched.
- Control (explicit approvalPolicy/reviewer/sandbox params only): the escalated `touch` ran with no client request, the
  outside write and the localhost connect succeeded, `HOSTILE_VAR` printed, the MCP server launched
  (`mcpServer/startupStatus/updated`, marker created), and `thread/start` reported the widened `writableRoots`,
  `networkAccess: true` and `runtimeWorkspaceRoots`.
- Complete pin set on argv `-c`, in the `config` map (including `projects."<root>".trust_level = "untrusted"`, which overrides
  the user-config `trusted` entry) and as explicit params, for a fresh `thread/start` and then a fresh-process
  `thread/resume`, both turns: the escalated `touch` raised a client approval request (the project allow rule did not
  apply); the outside write failed (`Operation not permitted`); the connect failed (`curl` exit 7); `HOSTILE_VAR` was absent;
  no MCP startup notification and no marker; no `hook/*`, `item/autoApprovalReview/*` or `guardianWarning`
  notification; `hooks/list` under the argv pin returned no hooks (the control listed the project hook as trusted). Both
  responses reported `on-request`, `user`, `workspaceWrite` with `writableRoots: []`, `networkAccess: false` and
  `runtimeWorkspaceRoots` equal to the repo only. The SHA-256 of `CODEX_HOME/config.toml` and the absence of `CODEX_HOME/rules`
  were identical before and after both runs.

**Caller approvals, file changes, rules and denial evidence (Stage 3; archived transcripts `tr-*`, `wt-*`, `cfg-rl1`, `ex1-exec-approval-policy-never.txt`)**
- Rules files: Codex loads regular files directly in `rules/` named `*.rules`, hidden ones included. A `.txt` or extensionless
  file, a subdirectory's file, an upper-case `.RULES`, a symlinked file and a symlinked directory were not loaded
  (an allow rule in each ran the command as a request). The default `decision` of `prefix_rule` is `allow`, and
  `"al" + "low"` and `"\x61llow"` evaluate to it (`codex execpolicy check`), so Harflow accepts a rules file only if it consists of
  literal `prefix_rule` calls with an explicit `"prompt"` or `"forbidden"` decision (an opt-in test checks the grammar against
  `codex execpolicy check`).
- Project trust overrides: a `thread/start` `config.projects` map with an `untrusted` entry for the working directory, every
  ancestor up to `/` and the Git root (12 to 13 entries) was accepted and wrote nothing to `CODEX_HOME/config.toml`. With a hostile
  project allow rule, the repository trusted, only its parent trusted, or only a subdirectory used as the working directory trusted, the
  map made the escalated command ask the client in every case; without the map the rule applied and the command ran silently. A
  workspace-write start without the map added a trust entry to `config.toml` in the parent-trusted control.
- Linked worktrees (`git worktree add`): Codex applies the **main checkout's** trust to a linked worktree unless the worktree has an
  explicit entry (a trusted main checkout made the worktree's project allow rule apply; so did a trusted worktree). Untrusted overrides for
  the worktree's directory chain alone, and with the main root added, both made the rule ignored; both trusted plus the main root added was ignored too.
  Harflow still adds the main checkout and the common directory, with their ancestors, and scans the main checkout's `.codex/rules`, because the `gitdir:`
  line of the worktree's `.git` file is repository content: it is accepted only as `<common>/worktrees/<name>` with Git's own back-link and `commondir`,
  and a submodule-style or otherwise unverifiable `.git` file refuses.
- `git rev-parse --show-toplevel` still honours `core.worktree` from the repository's own config (`-c core.worktree=` does not
  override it) and reports a root outside the tree, so the Git root is accepted only when it is the nearest ancestor of the canonical
  directory that holds `.git`.
- Exec path denial evidence: an escalation refused under `approval_policy="never"` produces **no JSON event**; the only trace is
  the stderr line `ERROR codex_core::tools::router: error=approval policy is Never; reject command — you cannot ask for escalated
  permissions if the approval policy is Never` (also the text the model sees).
- App-server denial evidence: an exec-policy `forbidden` command produces no item; the stderr router line
  `exec_command failed: CreateProcess { message: "Rejected(\"`<command>` rejected: <justification>\")" }` is the only server-side
  trace. A declined item (`status: declined`) is Harflow's or the caller's decision, which only the approval ledger can tell apart.
  Tool processes hold pipes to the native process, not the server's stderr (opt-in test).
- File-change approvals: a patch is joined by `itemId` with the earlier `item/started` of the same thread and turn; the completed item's
  `changes` equal the started ones; `grantRoot` was always null and an accept did not grant the root.

## Claude plan-mode writes (Harflow remeasurement, 2026-10-26)

Measured installed Claude Code **2.1.310** with isolated HOME and CLAUDE_CONFIG_DIR, a localhost
fake Anthropic API and deterministic tool-use responses; no paid model requests. Repeat with:

```sh
HARFLOW_CLI_INTEGRATION=1 uv run pytest -q tests/test_claude_plan_files_cli.py
```

Both default `CLAUDE_CONFIG_DIR/plans` and a project `plansDirectory` setting allowed a Write to
the prescribed random plan filename disclosed in the CLI's system prompt. Ordinary repository
and outside-file Writes were explicitly denied in plan mode and neither file appeared. Both
scripted runs exited zero. The installed implementation accepts `plansDirectory` only within the
project root; it cannot select the engine's external task scratch through that setting. This
measurement qualifies the plan-write disclosure/tracking case, not an OS boundary or arbitrary
CLI versions/configurations. Runtime history/log writes are separate from agent-tool permissions.

The review-follow-up measurement uses those same two isolated CLI probes to feed actual stream
Write/Edit/result events through the bounded metadata candidate extractor and worker-owned
correlation seam. Both cases produce exactly the observed plan reference and its directory/inode
identity; denied ordinary writes produce no reference. No argv, permission-mode or transport
behavior changed. Separate fake-process contention tests verify that blocked artifact registration
does not block stdout/results, alter `output_drained`, or turn successful execution into failure.

## Codex and linked worktrees (codex-cli 0.163.0, 2026-10-27)

Measured model-free with `codex sandbox -P :workspace -- sh -c ...` (the seatbelt sandbox that backs
`workspace-write`; no `codex exec`, no model call), in a repository and in a linked worktree of it, both
outside the temporary directories that the sandbox makes writable by default:

- Writing a file in the working tree: allowed in both.
- Writing under the repository's `.git` in the normal checkout: `Operation not permitted`.
- In the linked worktree: writing under its own gitdir (`<repo>/.git/worktrees/<name>`) and under the
  common directory (`<repo>/.git`): `Operation not permitted`.

So a worktree changes nothing for Codex at `write_in_repo` and `publish`: Git writes (commits, branch
changes) are denied there exactly as in a normal checkout, and work at `unrestricted`. Decision: parity,
no extra writable roots, because granting the common directory would also expose `.git/config` and
hooks. Limit: this is the `codex sandbox` profile, not a `codex exec` run; requalify it through `codex
exec` before claiming more. Harflow records the worktree path as the task's `repo_path`, so Codex's
working-directory and writable-root handling see the worktree.

## Claude repeated `--add-dir` (claude 2.1.310, 2026-10-27)

Measured without a model call: `claude -p --output-format stream-json --verbose --permission-mode acceptEdits`
with an isolated `HOME` and `CLAUDE_CONFIG_DIR`, a localhost fake Anthropic API (the scripted `Write` tool-use
harness of `tests/fake_anthropic_api.py`) and each run in an empty working directory outside the target
directories. `--add-dir <dir>` was given once per directory, before `--`, and `permission_denials` of the `result`
event was read afterwards.

| Case | Result |
| --- | --- |
| two `--add-dir`, `Write` into each | both files written, empty `permission_denials` |
| three `--add-dir`, `Write` into each | all three written, empty `permission_denials` |
| two `--add-dir`, `Write` into a third, ungranted directory | not written; one denial naming that file |
| no `--add-dir`, `Write` into a directory outside the working directory (control) | not written; one denial |
| two `--add-dir` together with `--allowedTools` and `--disallowedTools` | both written, no denial |

So repeated `--add-dir` before `--` makes each named directory writable under `acceptEdits`, and only those
directories. This is what `ClaudeBackend.with_shared_directories` relies on: one `--add-dir` for the task
scratch, then one each for the shared chain and group scratch (see [shared scratch](shared-scratch.md)),
and `assert_safe` accepts exactly that ordered list. Limits: `acceptEdits` Write only (Edit shares the permission
layer but was not separately driven); the measurement says nothing about `bypassPermissions`, where the grant is
redundant, and nothing about Bash, which `acceptEdits` auto-approves only inside the working directories it
knows. A grant is a statement of what Harflow added, not a sandbox.

Codex repeated `--add-dir` is already measured (see the extra writable paths above); the implementation adds the
shared folders to the same list, ahead of any extra grants, and keeps all four scope checks over every one of them.

## System-prompt seams for the scratch note (2026-10-27)

Harflow gives agents a short static note about the scratch variables (`system_guidance.TEXT`, at most 600 bytes,
no paths; the variables carry the per-run paths). The caller's prompt is never prefixed or rewritten: a leading
`/slash` command or skill invocation must stay first in the user message, and the handoff is delivered verbatim. So
the note travels only through a backend's own system-prompt channel, and only where one was measured that grants
nothing else. All measurements used isolated `HOME` and config directories and a localhost fake model API, with
no model calls, recording each request body; a marker string stood for the note.

| Backend (version) | Seam | Measured |
| --- | --- | --- |
| claude 2.1.310 | `--append-system-prompt <text>` before `--` (also in the stream-json live shape and with `--add-dir`, `--allowedTools`, `--model`, `--effort`, `--max-turns`) | The text is appended to the system prompt on a fresh session. A resume reuses the session's own snapshot: resuming without the flag still shows the original text, resuming with a different text shows the original, not the new one, and a session started without it never gets it from a resume (`--system-prompt-snapshot off` changes this, but Harflow never passes it). A `/slash` prompt expands exactly as without the flag, on start and on resume. Values of 100 KB and 600 KB were delivered intact (the note itself is under 600 bytes), also from `--append-system-prompt-file`. The text is stored in the session transcript. `first_turn_sticky`. |
| codex-cli 0.163.0 `exec` | `-c developer_instructions=<JSON string>` before `--` | A JSON-encoded string is a valid TOML basic string and arrives as one developer-role message ahead of the user prompt (an unquoted value that is not valid TOML is taken literally; `true` parses as a boolean and delivers nothing, so the pair is always JSON-encoded). The rollout file keeps it: a resume without the flag still shows it, and a resume with a different text shows the original only. `instructions` and `model_instructions_file` also deliver text, but each shrank the request body by about 22 KB (the base instructions were replaced, not added to; `developer_instructions` added only its own length), and `experimental_instructions_file` and `base_instructions` delivered nothing, so none of them is used. `first_turn_sticky`. |
| codex-cli 0.163.0 `app-server` | `developerInstructions` on `thread/start` | Accepted by the protocol schema and delivered on `thread/start`; `thread/resume` accepts the field too but the resume path does not send it, because the thread already holds the original. Used only on the caller-approvals transport, where the allowed-fields table is extended by exactly this field and `validate_context` refuses any text but the note. `first_turn_sticky`. |
| opencode 1.18.21 | `OPENCODE_CONFIG_CONTENT={"instructions":["<absolute path>"]}` pointing at a read-only file under `<state root>/guidance/` | The file's text joins the first system message on every run, start and resume, and is not stored in the session, so a changed note applies at once: `per_run`. A path that does not exist delivers nothing and does not fail the run; literal text in `instructions` is read as a path, not as text. A caller-set `OPENCODE_CONFIG_CONTENT` would be replaced, so the seam is skipped (and disclosed as unavailable) when one is set. Harflow's only environment contribution on this backend is this exact shape (`validate_invocation_env`). |
| vibe 2.26.1 and 3.0.2 | none shipped | 2.26.1: nothing delivers text without also granting file access (`--add-dir` with an `AGENTS.md`) or writing into the repository or `VIBE_HOME`; `VIBE_SYSTEM_PROMPT_ID` did not deliver. 3.0.2 (measured the same way) has two seams. `VIBE_AGENT_PATHS` with a shadow agent profile appends text to the system prompt on every run, but it replaces the built-in profile of the same name, which would couple the note to the permission profiles Harflow's freedoms rely on; the hidden `--agent-socket` delivers it too but cannot be combined with `--workdir`. Neither is shipped: vibe declares `system_guidance: None`, and the shadow-profile seam is a follow-up candidate if its profile policy can be kept identical to the built-ins. |
| agy 1.4.7 | none | No system-prompt flag; not shipped. |

Disclosure is per run in the task's `scratch.system_guidance` (`delivered`, `reused_from_session` or `unavailable`) and
in each backend's `capabilities.system_guidance` (`first_turn_sticky`, `per_run` or none). `reused_from_session` is
claimed only when the resumed task itself recorded the note as delivered or reused; a session that began before the note
existed reports `unavailable` on resume. A failure to attach the note never blocks a launch. Workflow builders and native
children carry none.

Hazards recorded by the measurement: a note delivered once is sticky on claude and codex, which is why it is static
(paths come from the environment, so a stale copy is never wrong); the text must be ASCII and free of a leading `-` (a
value that starts with `-` is refused by the strict option walks); and each backend's `assert_safe` accepts exactly the
note, once, in exactly one position, so a caller-controlled string can never take that seam.

## Durable execution accounting qualification

The bounded `accounting_evidence` adapter seam consumes the accumulator produced by the
measured stream parsers above. It does not re-read transcripts, infer a requested model from an
observed model, or alter classification/live-input closure. Claude per-result usage and opencode
per-step usage are accumulated; Antigravity keeps its measured per-process cumulative result.
Codex `exec` retains its latest `turn.completed` usage; it is execution-scoped only for a fresh
single-turn execution. App-server and multi-turn scope remain unknown. Vibe's measured stream
has no usable token accounting and returns unavailable.

Initial Claude/opencode/Antigravity observations expose their recognized input/output fields;
Claude additionally exposes cache-read/cache-creation, and Codex its recognized cached-input
field. Unsupported or absent categories remain null. The existing measurements do not establish
that every reported category is non-overlapping, nor that resumed session counters are disjoint
from earlier executions. Accordingly the adapters do not publish comparable totals; resumed
usage has unknown scope and is not additive. A deterministic synthetic fixture can exercise the
qualified-total contract but does not qualify a real CLI. No new paid qualification was performed.

No current production adapter qualifies caller-input pause intervals. Question/approval timestamps
and idle/background waits do not establish that a matching execution stopped work. Statistics
therefore uses elapsed execution duration with `elapsed_may_include_waits`. The accounting seam
can retain bounded, explicitly qualified matching-execution intervals; union/clamping tests are
algorithm evidence only, not production pause qualification.


## Claude Git preparation grants (2.1.312, 2026-10-28)

The installed Claude Code 2.1.312 passed the no-paid candidate suite using the localhost fake
Anthropic API and isolated HOME/CLAUDE_CONFIG_DIR: 36 tests across `test_claude_denials_cli.py`,
`test_claude_caller_approvals_cli.py`, `test_claude_approval_resume_cli.py`,
`test_claude_write_approvals_cli.py` and `test_claude_commands_cli.py`. The measured qualification
row therefore enables the same caller approval tools, write approvals, path prompts, denial texts
and command grants as 2.1.310. No paid model or external Git remote was used.

At `publish`, exact grants for `git -C . fetch origin develop`,
`git -C . pull --ff-only origin develop` and `git -C . switch -c feature/prep` executed against a
local Git fixture: fetch/pull created FETCH_HEAD and switch changed the observed branch. With those
grants, `git -C . reset --hard` and appending `--force` to the granted pull were refused by the
real harness. The test also checks permission preview includes the exact grants. Unit cases
refuse wildcard patterns, other Git subcommands, config overrides, force/rebase forms, URL/refspec
operands, outside paths and launcher nesting. These measurements establish the tested matching
behavior, not an OS Git boundary: `publish` independently permits commit/push, and Git config,
hooks, remote helpers and credentials remain effective. Saved workflow candidates require an
explicitly authorized grant update; dynamic targets/branch names need concrete per-run grants.


### Claude plan files (2.1.312, 2026-10-28)

The candidate-enabled `tests/test_claude_plan_files_cli.py` passed both no-paid CLI probes on
2.1.312: plan mode created and updated the actual built-in plan file in the default config/plans
location and the project-configured `.claude/plans` location. Ordinary repository and outside
writes, and writes to settings, settings.local, hooks, agents, commands, skills and `.mcp.json`,
were refused. The output-route change added system guidance and version evidence, not backend
argv changes. This preserves the disclosed plan-file exception; it is not a universal read-only
filesystem claim.


A separate no-grant 2.1.312 probe at `publish`, with caller approvals disabled, observed
`This command requires approval` for all four exact commands: `git -C . fetch origin develop`,
`git -C . pull --ff-only origin develop`, `git -C . switch develop`, and
`git -C . switch -c feature/prep develop`. Branch preparation is therefore not implicitly allowed
by acceptEdits on this measured harness. Dynamic branch names need a concrete grant selected for
the run; static fetch/pull grants alone do not complete the saved workflow migration.

### Automatic permission probe bundles

The production callbacks were measured against Claude Code 2.1.312 and codex-cli
0.163.0 on macOS using `HARFLOW_CLI_INTEGRATION=1 uv run pytest -q
tests/test_qualification_probes.py` (15 passed). These runs use spawned qualification
workers, isolated vendor configuration, fake credentials, synthetic local providers,
and `sandbox-exec` restricted to disposable writes and the fixture's exact loopback port.
They make no paid model calls. A separate real Claude plan-mode check reconstructs a
complete inherited specification through paged Read calls, byte-identically, without
Bash classification or Harflow tools (`tests/test_child_spec_delivery_cli.py`).

Caller approvals, write approvals, path prompts, command matching and denial text have
independent outcomes. Synthetic protocol/provider drift tests verify that one failed
feature does not erase unrelated observed successes. Unavailable OS isolation refuses
auto qualification; no support is inferred for unmeasured platforms or unimplemented
backend permission adapters. See `docs/harness-permissions.md` for the cache and runtime
invalidation boundaries.

## Claude publish PR/review command families (2.1.312, 2026-10-28)

Measured installed Claude Code 2.1.312 without paid model calls using an isolated
HOME/CLAUDE_CONFIG_DIR, the localhost fake Anthropic API and a harmless `gh` executable
that writes an execution marker instead of contacting GitHub:

```sh
HARFLOW_CLI_INTEGRATION=1 uv run pytest -q tests/test_claude_commands_cli.py -k publish_github
```

All 23 probes passed. At `publish`, each of `gh pr create`, `edit`, `view`, `comment`,
`review`, `checks`, `diff`, `list` and `gh repo view` executed, both alone and with
`--help`, without permission denial. `gh pr merge`, `gh repo delete`, `gh release create`,
`gh api repos/example/repo/pulls` and `gh api --method POST repos/example/repo/pulls`
produced classified `approval_required` denials and did not execute the stub.
This qualifies Claude's matching and denial behavior, not GitHub authentication,
request success or actual PR mutations. These built-in grants do not depend on the
separately qualified `allowed_commands` escape hatch.

The shipped allow-list is constructed from exact command families in
`backends/claude.py::PUBLISH_COMMANDS`; enforcement's `mechanism` contains the actual
CLI patterns. It grants no raw `gh api` calls, including GET: the prefix matcher cannot
prevent a caller from appending a different HTTP method or fields that cause POST.
Supported PR/repository reads use `gh pr view --json` or `gh repo view --json` instead.
The exclusions describe grants Harflow omits, not an OS boundary: inherited Claude
settings, granted programs and repository code may enable additional behavior.

## Claude exact branch preparation at write_in_repo (2.1.312, 2026-10-28)

The installed Claude Code 2.1.312 passed 18 no-paid localhost fake-provider probes in
`tests/test_claude_commands_cli.py -k git_preparation`, covering both `write_in_repo`
and `publish`. Exact fetch, ff-only pull and non-force branch creation executed in disposable
clean Git repositories with local bare remotes; reset, force variants and commands with no
grants were denied. At `write_in_repo` the existing direct commit/push deny rules remain present.
Broad patterns, config overrides, outside paths, destructive commands and launcher nesting
remain refused by the grammar unit tests. This extends the earlier publish-only preparation
measurement; it does not qualify an OS sandbox or arbitrary vendor versions.

`test_saved_prepare_branches_workflow_clean_multi_repo` additionally saves and reloads an
isolated Multi-platform Feature Implementation fixture, executes the actual workflow scheduler
with deterministic in-process orchestrator decisions and an actual Claude worker, and observes
fetch, ff-only pull and feature branch creation in two contained clean repositories with no
caller approvals or permission denials. Both branches are verified afterward. It does not mutate
or certify the user's existing saved workflow: that definition still needs an explicitly authorized
per-run grant update for its concrete repositories and branches.

Initial-dispatch and permission-preview checks refuse dirty transition targets before spawning
an agent, without stashing/resetting. The check is not a lock; later dirty races and Git config,
hooks or remote helper behavior remain outside this measured grant boundary.

## Codex scoped read-only group reports (0.165.0, macOS, 2026-10-28)

Actual `codex sandbox` and localhost fake Responses API `codex exec` probes established native
permission profiles on installed codex-cli 0.165.0. A profile extending `:read-only`, with
`:root = read`, exactly one canonical group-directory `write` rule and network disabled, allowed
report creation, update and same-group peer reads while denying repository, repository `.codex`
configuration, unrelated-group, outside, traversal and symlink-escape writes. The repository stayed
the working directory. Fresh, resumed and new continuation sessions preserved those boundaries;
a different group's profile could read the original report but could not write its directory.
No paid model calls were made.

The production configurator is verified by `HARFLOW_CLI_INTEGRATION=1 uv run pytest -q
tests/test_codex_group_reports_cli.py`: native create/update plus denial checks across fresh,
resume, continuation, peer and different-group sessions. It also checks that the actual writable
group path appears in the native model-visible permission instructions. This qualifies macOS
0.165.0 only. Other versions/platforms have no auto-probe for this feature and refuse explicitly.

Canonical argv removes the legacy `-s read-only` pair and adds `-c default_permissions` plus one
whole inline TOML `permissions` table; the directory is not added as a workspace root. Dotted
`-c` keys with quoted filesystem path components were rejected because quotes became part of
the path. Legacy `sandbox_mode` configuration can override profiles, so the supported execution
profile conservatively refuses legacy sandbox settings, existing permissions/profile selectors,
project configuration, rule directories, managed configuration and unsafe execution configuration.
Runtime/configuration identities and the group-directory identity are checked before execution;
these observations are not an atomic lock against independent concurrent modification. Ordinary
read-only tasks keep their previous profile. Scoped output does not authorize caller approvals,
native children, additional writable paths or a change in repository access.

## Cursor Agent CLI (2026.10.22-27dcf5f, 2026-10-28, macOS arm64)

`cursor-agent` and `agent` are the same wrapper. Both are symlinks into a versioned
directory. Harflow invokes the symlink name and does not resolve it. `agent --version`
prints only `2026.10.22-27dcf5f`.

Signing into the Cursor editor does not sign in this CLI. The credential `agent status`
reads is for the current `HOME`. After `agent login`, `agent status --format json` in a
shell with the real home directory reports an access token and a refresh token. The same
binary with `HOME` pointed at another directory reports `Not logged in`. `CURSOR_API_KEY`
is the non-interactive alternative. Confirm with `agent status` before headless dispatch.

Parser, still true on this version: `-p` is a boolean (`-p=hello` is rejected). The prompt
is one positional after `--`, so a prompt that looks like a flag is preserved. Stdin is
read only when it is not a terminal and there is no positional prompt, and that text is
trimmed. Unknown `--effort` is rejected. `--mode` accepts `plan` and `ask`. `--sandbox`
accepts `enabled` and `disabled`. `--trust` and `--workspace` are accepted. A missing
workspace path is rejected before auth. `--output-format` in the shipped writer allows
`text`, `json`, and `stream-json`; Harflow launches only `stream-json` and refuses the
other two because that is the stream it parses. There is no live follow-up stdin protocol.

`--new-session-id` wants a UUIDv4 and refuses combining with `--resume`. Resume of an
unknown id was not observed past auth, so `resume_may_start_fresh` is true. `--system-prompt`
is a file path. Delivery of that file into a model request was not observed, so system
guidance is not shipped.

Global MCP config is `~/.cursor/mcp.json`, key `mcpServers`, with `command`, optional `args`,
and `env`. `agent mcp list` accepts JSONC and does not rewrite it. Harflow refuses a file
`json.loads` cannot round-trip. Registration writes only the global file. `agent mcp enable`
approves the server for the working directory of that command (setup uses the home directory)
and does not rewrite `mcp.json`. `agent mcp disable` does not remove the entry. Whether an
MCP child observes `env.PATH` was not measured; the entry still carries it because that is
the registration contract.

Tool behavior on one authenticated scratch repository, print mode, 2026-10-28. Each row is
one run. "Rejected" here means the model did not perform the step, not a captured tool error,
unless the filesystem shows the write or the commit:

| Launch | Read | Write in repo | Write outside repo | Commit | Push | Network file |
| --- | --- | --- | --- | --- | --- | --- |
| `-p` | yes | yes | yes | no | no | no |
| `-p --mode plan` | no tool call | no | no | no | no | no |
| `-p --mode ask` | yes | no | no | no | no | no |
| `-p --sandbox enabled` | yes | yes | yes | yes | no | no |
| `-p --sandbox disabled` | yes | yes | yes | no | no | no |
| `-p --force` | yes | yes | yes | yes | no | yes |

Plan mode declined without a tool call. Ask mode read the file and said it would not write.
Default print mode wrote outside the repository. `--sandbox enabled` did not confine that
write and did create a commit. `--force` also fetched a public URL. Push to a local bare
remote was not observed in any of these runs, so Harflow does not claim push is blocked.
None of these flags is an OS sandbox Harflow can report. `write_in_repo`, `publish`, and
`unrestricted` therefore share one `-p` argv, and `read_only` only adds `--mode plan`.
Caller approvals, `allowed_commands`, a turn cap, reasoning effort, and extra writable paths
are refused. The user's `~/.cursor/mcp.json` was unchanged by these runs.


Offline normalization coverage (2026-10-29) uses
`tests/fixtures/cursor/success-result.jsonl`, reconstructed from this installed CLI's
stream-json writer, not a captured live transcript. Its `assistant.message.content`
text blocks become Monitor assistant text; the result event is ignored by normalization
to avoid repeating the final reply. Malformed blocks and unknown event types are ignored.
No Cursor stderr quota or recoverable-cutoff signal is qualified, so those protocol
hooks return conservative defaults. This fixture check adds no live CLI qualification.


## Vibe HTTP 402 diagnostics

Re-measured Vibe 3.0.1 on macOS, 2026-10-28, using the existing isolated HOME/VIBE_HOME
loopback fixture in `tests/test_vibe_cli.py`, with no inherited provider credentials and no
paid calls. The fake provider returned HTTP 402 and
`{"error":{"message":"Usage limit exceeded","type":"usage_limit"}}`. Vibe made one request,
exited 2, emitted only the user turn on stdout, and wrote two stderr lines:

```text
Error: Client error '402 Payment Required' for url 'http://127.0.0.1:12345/v1/chat/completions'
For more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/402: Usage limit exceeded
```

The port is sanitized; identifiers were omitted. The captured sanitized fixture is
`tests/fixtures/provider-errors/vibe-3.0.1-http402.json`. The old one-line parser missed this
pair and the structured parser rejected 402 before checking the provider's quota code.
The Vibe adapter now recognizes the exact adjacent stderr pair and preserves HTTP 402
without a URL or response body. Unknown 402 details retain a generic provider-error
diagnostic instead. Structured Vibe error tests are synthetic compatibility cases, not a
claim that Vibe 3 emitted a structured terminal error in this probe. This measurement
qualifies the CLI event path with a scripted provider, not every real provider's 402 meaning.

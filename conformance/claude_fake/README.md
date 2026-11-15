# Scripted `claude` fake

A standard-library Python program (3.11 or later) that stands in for the `claude` CLI on a lifecycle conformance
case's `PATH`. It speaks the stream-json protocol the engine's Claude adapter parses, replays recorded lines from a
scrubbed bundle and scripts the failure modes a real CLI cannot produce on demand. It is **protocol shape only**: no
model behaviour, no provider call, no network.

It exists for E2-C and E2-D lifecycle cases (complete, nonzero exit, detached resume, cancel, owner death, survivors)
against any engine that launches `claude` from `PATH`: the Python engine today, the Rust engine later. It is not
imported by any engine and no engine build, test or release depends on it.

## Layout

| Path | Content |
| --- | --- |
| `fake_claude.py` | the program |
| `recordings/` | the scrubbed recording bundle, its `SHA256SUMS` and `README.md` (provenance, scrub rules, retention table) |
| `../lifecycle/participant.py` | the observer client the fake copies beside itself (registration, `kill_parent`, `bound_lifetime`) |

## Installing and pinning it

The `path-fake` seam (`conformance/lifecycle/seams.py`) does this per case; another harness can do the same:

0. Check the bundle against the pinned digest of `recordings/SHA256SUMS` (`EXPECTED_SHA256SUMS` in
   `conformance/lifecycle/seams.py`) and that every listed file matches before installing anything.
1. Create a case directory with a `bin/` folder. Copy `fake_claude.py`, `participant.py` and the whole `recordings/`
   folder into one installed folder (the seam uses `<case>/fake/`).
2. Write `fake.json` beside `fake_claude.py` (below). It is the fake's only configuration; the fake reads no
   environment selector.
3. Write `<case>/bin/claude`, a short launcher that must stay executable: `#!<absolute python 3.11+> -IB` (the interpreter
   path must not contain whitespace), then `import runpy` and `runpy.run_path('<installed>/fake_claude.py', run_name='__main__')`.
   `-I` drops the script directory from `sys.path` and `-B` stops bytecode being written into the installed folder (a `__pycache__` there counts as drift), so the fake finds `participant.py` and `recordings/` from its own file path.
4. Run `claude --version` through the launcher once before the first engine call. The first execution of a new script
   file can stall for many seconds under host execution policy (4 to 30 s measured on macOS); doing it at install
   keeps that cost out of launch deadlines. The probe registers nothing and writes no ledger.
5. Hash every installed file, the launcher, the interpreter path, `sys.version` and the interpreter binary; re-hash before
   each launch and refuse on drift (`PathFakeSeam` records this as `fake_identity`); re-verify the interpreter's resolved path and digest too. Put `<case>/bin` first on the case
   `PATH` (`<case>/bin:/usr/bin:/bin`) and check that `which claude` is the launcher before every engine call.

The engine must be asked for an explicit `--freedom unrestricted` launch (owner decision O1, option 5); no selector,
allow-list or evidence file is involved. Do not pass `--caller-approvals` or `--allowed-commands`: the fake does not
speak the approval transport (`--permission-prompt-tool` is refused) and the seam refuses such launches.

## Argument vector

Accepted: `-p/--print`, `--output-format`, `--input-format`, `--verbose`, `--include-partial-messages`,
`--forward-subagent-text`, `--permission-mode`, `--max-turns`, `--model`, `--effort`, `--max-budget-usd`,
`--setting-sources`, `--strict-mcp-config`, `--mcp-config`, `--no-session-persistence`, `--session-id`, `--resume`,
`--tools`, `--allowedTools`, `--disallowedTools`, `--append-system-prompt`, `--add-dir` (repeatable), `--agents`,
`--settings`, `--permission-prompts`, `--version`, and `--` ending the options. This is the union of the engine's
Claude option tables plus the flags the recordings used. Value options take exactly one following token;
attached `--flag=value` spellings are refused, like the engine's own spelling checks.

Refused with one stderr line and exit 1: an unknown option, `--session-id` with `--resume`, a missing `-p`, an output
format other than `stream-json` or `stream-json` without `--verbose`, a permission mode other than `default`, `plan`,
`acceptEdits` or `bypassPermissions`, a repeated non-repeatable option, `--version` with other arguments, and
`--permission-prompt-tool` ("approval transport is not faked"). The wording is scripted, not recorded.

`claude --version` as the whole vector prints `recordings/version.txt` and exits 0 before anything else happens: no
bundle hash, no registration, no ledger. Every other invocation that is not an agent run writes a ledger record with
`kind: refused` and does not register.

## Input

- Live input (`--input-format stream-json`): newline-terminated user frames on stdin
  (`{"type":"user","message":{"role":"user","content":[{"type":"text","text":...}]}}`), one turn per frame. Stdin EOF
  ends the run with the recorded exit code of the mode (0 for most). EOF before any frame prints nothing and exits 0.
- One-shot: the positional prompt (after `--`), otherwise the whole of stdin.
- Bounds: 64 KiB per frame and 1 MiB total input; a breach exits 70.

## Script marker

The behaviour is chosen by the text of the first prompt (the first text block of the first frame, or the one-shot
prompt): `HARFLOW_FAKE_CLAUDE:` followed by one JSON object, parsed with `raw_decode` so trailing text is ignored.
No marker means `complete`. A malformed marker, an unknown mode or an unknown key exits 70 with a ledger record.

| Mode | Keys | Behaviour | Source |
| --- | --- | --- | --- |
| `complete` | | the recorded success turn for each frame; `r05_resume` when `--resume` is given, `r01` for one-shot | recorded |
| `multiturn` | | first frame replays turn 1 of `r03`, each further frame its second turn; exits 1 at EOF like the recording | recorded, non-gating |
| `tool_use` | | `r06` | recorded, non-gating |
| `permission_denied` | | `r07` | recorded, non-gating |
| `error_model` | | `r08` lines and its stderr line, then exit 1 | recorded, non-gating |
| `interrupt` | | `r09` up to the signal point, then waits; SIGINT emits the recorded `user` echo and `error_during_execution` result and exits 0 | recorded, non-gating |
| `nonzero` | `exit_code` (1 to 255, default 7) | the `r08` error result and stderr line, then exit `exit_code` immediately | scripted |
| `hold` | `gate` (absolute path), `survive_eof` | the success turn up to its first `assistant` line, then waits until `gate` exists, replays the rest and exits 0; stdin EOF while waiting exits 0 unless `survive_eof` | scripted |
| `escape` | as `hold`, plus `descendant_holds_pipes` | as `hold`, plus one descendant that calls `setsid()`, registers with role `descendant`, redirects stdio to the null device unless `descendant_holds_pipes`, and exits at the gate, the stop file or its lifetime bound | scripted |

SIGINT is always handled: while a turn is open it emits the `r09` post-signal lines and exits 0; while idle the fake
re-raises it with the default action. SIGTERM keeps its default action, so the process dies by signal (the real CLI's
SIGTERM behaviour is unmeasured). Every write (stdout, stderr, the `--version` line and the refusal line) goes through one routine that loops over partial writes; a write error ends the run with exit 74 (recorded in the ledger when there is one; `--version` never has one).
`gate` must resolve inside the `workspace` named in `fake.json`; it is validated before any output, so an out-of-workspace gate refuses with nothing on stdout.

Replay substitutes the recorded `session_id` with the `--session-id`/`--resume` value, the recorded `cwd` with the
actual one, and `system/init` `model` with `--model` when given. Everything else is the bundle's bytes (a line is
re-serialized only when a substitution changed it). `pace` (a factor applied to the recorded line gaps, each capped at
2 s) defaults to 0: no sleeps.

## `fake.json`

Read from the installed folder. A missing file means no observer, no ledger and no workspace; a file that exists but cannot be parsed or has an invalid `lifetime` makes every run except `--version` exit 70.

| Key | Meaning |
| --- | --- |
| `observer_socket`, `nonce` | register with the observer (role `fake`) before any output and take its acknowledgement |
| `ledger_dir` | where the ledger is written |
| `control_dir` | where control files are polled; `<ack token>.kill-owner` asks the fake to end its own recorded parent |
| `stop_file` | when this file exists any waiting mode exits 0 without more output (cleanup) |
| `workspace` | the directory a prompt-supplied `gate` must resolve inside |
| `lifetime` | required when `fake.json` exists: an integer from 1 to 600, the hard lifetime bound in seconds; the fake and any descendant exit 70 by themselves |
| `pace` | see above |

The bundle is verified against `SHA256SUMS` after the lifetime bound is set and before registration; any difference,
extra file or symbolic link exits 70.

## Ledger

One file per run in `ledger_dir` (the name is opaque: match a ledger to a fake by its `registration` record), opened exclusively, one JSON object per flushed line. It
is untrusted corroboration written by the fake itself: exit codes and settlement come from the engine's public result
and the observer, never from it. A ledger without an `exit` record is *incomplete* (death by signal, lifetime bound),
not malformed. A final line without a newline is reported by the seam as trailing bytes, never silently dropped.

| `event` | Fields |
| --- | --- |
| `start` | `kind` (`agent` or `refused`), `registered` (always false here), `pid`, `ppid`, `argv`, `cwd`, `session_id`, `session_flag` (`--session-id` or `--resume`), `bundle_sha256` (digest of `SHA256SUMS`), `fake_sha256`, `python`; a refused record carries `reason`. The ledger is opened before registering, so a failed registration still leaves one |
| `registration` | `ok`, and on success `ack_token`, `lstart`, `ppid`; on failure `error` |
| `mode` | `mode`, `scripted`, `recording`, `spec` |
| `io` | `direction` (`stdin`, `stdout`, `stderr`), `bytes`, `sha256`, `raw_hex`, and for writes `written` and `error`; after 1 MiB of raw bytes records carry `digest_only: true` and no `raw_hex`, and are not verified |
| `signal` | `signum`, `turn_open` (SIGINT only) |
| `kill-owner` | `requested`, `sent` |
| `descendant` | `pid` |
| `marker-error` | `reason` |
| `exit` | `code`, optional `note` |

## Exit codes

0 success or stdin EOF; 1 recorded error (`r03`, `r08`) or a refused argument vector; 7 default `nonzero`; 70 fake-internal
refusal (bundle tamper, malformed script, gate outside the workspace, bound exceeded, registration failure, lifetime
bound); 74 write error; death by signal on SIGTERM.

## Limits

Protocol shape only. SIGTERM behaviour, refused-argument wording, error exit codes other than the recorded ones and
every bound are scripted, not measured on the real CLI. `escape` is a fixture for E2-D survivor reporting, not evidence
that reporting works. The fake never touches the engine's state, is never imported by it, and registration is
bookkeeping owned by the harness, not an execution or filesystem permission.

# Recording bundle for the scripted `claude` fake

Scrubbed protocol recordings of the real Claude Code CLI, used by `../fake_claude.py` as its replay source. The bundle
is reduced to what a stream parser and a lifecycle conformance case need: protocol shape, never model behaviour.

## Provenance

- Recorded 2026-11-12 in an owner-approved paid session: CLI `claude` 2.1.322 (Mach-O arm64, sha256
  `a9d237949db816a0b7afd88a9a8e1df36b3a597a30f5a41d1c0539eb180a3450`), account login, `--model haiku`, isolated
  configuration (`--setting-sources ""`, an empty strict MCP configuration, fresh work directories). Eleven
  invocations (one lost to a harness defect and rerun), about 0.26 USD in total; each run carried
  `--max-budget-usd 0.05`.
- The unreduced captures are private and are not part of any repository. This bundle is derived from the first
  scrubbing pass of those captures plus the further reduction below.
- 10 runs, 177 stdout lines. `version.txt` is the single line printed by `claude --version`.

## Files

| Path | Content |
| --- | --- |
| `rNN_<name>/stdout.jsonl` | the run's stdout, one JSON object per line, compact separators, scrubbed and reduced |
| `rNN_<name>/stderr.txt` | the run's stderr (empty except `r08`) |
| `rNN_<name>/run.json` | `argv` and `cwd` as recorded (scrubbed), `exit_code`, `signals` (`[signal number, ms after start]`) and `t_ms` (milliseconds since start of each stdout line) |
| `version.txt` | `2.1.322 (Claude Code)` |
| `SHA256SUMS` | digest of every other file; the fake verifies it on every agent run and exits 70 on any difference |

| Run | Scenario | Exit |
| --- | --- | --- |
| `r01_oneshot` | one-shot, prompt after `--` | 0 |
| `r02_live_single` | live input, one turn | 0 |
| `r03_live_multiturn` | live input, two turns; the second hits the budget cap | 1 |
| `r04_session_start` | live input with session persistence (no replay mode uses it directly) | 0 |
| `r05_resume` | `--resume` of the `r04` session | 0 |
| `r06_tool_use` | a `Read` tool call that succeeds | 0 |
| `r07_permission_denied` | a `Read` outside the working directory, refused | 0 |
| `r08_error_bad_model` | unknown model | 1 |
| `r09_interrupt` | SIGINT after `system/init` (signal 2 at 1788 ms) | 0 |
| `r10_early_stdin_close` | stdin closed before any frame: no output | 0 |

### Exit codes

The first scrubbing pass zeroed every recorded return code, so `run.json.exit_code` is taken from the findings
table written with the recordings (`FINDINGS.md`) and was cross-checked against the raw return codes (integers
only, nothing else read from the raw captures): success (one-shot, live, resume) 0; permission refusal 0 (still
`success`, with a `system/permission_denied` line); unknown model 1 (`r08`); budget cap 1 (`r03`); SIGINT after
init 0; stdin closed before any frame 0. The `r09` signal time (1788 ms) is the one
recorded by the harness; it falls between the `status` line (284 ms) and the `user` echo (1793 ms). An exit code
alone never decides the outcome of a run: the interrupt exits 0, and an error result can say `subtype: success`
with `is_error: true`.

## Scrub rules

Rules 1 to 12 were applied to every string, dictionary key and value of the stdout, stderr and timeline captures
by the first pass (the pass had documented only rules 11 and 12; all twelve are transcribed here). Rules 13 to 18
are this bundle's reduction.

1. Paths: the recording-tree root, the harness work root, the shared code root and the user home directory (plain and
   dash-encoded as in Claude's project directory names) become `/workspace/recordings`, `/tmp/claude-UID`,
   `/workspace`, `/home/user`, `-workspace` and `-home-user`.
2. E-mail addresses become `user@example.com`.
3. UUIDs become `00000000-0000-4000-8000-%012d`, numbered per run in order of first appearance (case-insensitive).
4. Identifiers with the prefixes `msg`, `toolu`, `req`, `srvtoolu`, `rs` and `call` followed by at least ten
   alphanumeric characters become `<prefix>_SCRUBBEDnnnn`, numbered per run.
5. ISO-8601 UTC timestamps become `1970-01-01T00:00:00.000Z`.
6. Model identifiers `claude-<haiku|sonnet|opus>-...` become `claude-<family>-0-0`.
7. Unix socket paths of the form `/tmp/cc-socks/<n>.sock` become `/tmp/cc-socks/0.sock`.
8. Every number becomes `0` (`0.0` for a float) except values under the keys `num_turns`, `result_index`,
   `queued_turn_count`, `process_turn_index`, `index`, `contextWindow`, `maxOutputTokens`, `safety_stops` and `line`.
   Booleans and `null` are unchanged. Wall-clock durations in the run metadata are zeroed; `t_ms` is relative and kept.
9. A string under the key `signature` or `data` longer than 40 characters becomes `SIGNATURE_PLACEHOLDER`.
10. Dictionary keys are scrubbed with the string rules; list order and object key order are preserved.
11. Streamed delta fragments (`text_delta.text`, `input_json_delta.partial_json`, `thinking_delta.thinking`):
    fragments of one content block are joined, scrubbed with the rules above, then re-cut at the original fragment
    lengths (the last fragment takes the remainder). Line count and order are unchanged; fragment boundaries are
    approximate after substitution.
12. The timeline's `line` holds the same scrubbed object as `stdout.jsonl`.
13. `system/init` is reduced to an allowlist, in recorded order: `type`, `subtype`, `session_id`, `cwd`, `model`,
    `permissionMode`, `tools`, `claude_code_version`, `mcp_servers`, `uuid`. Removed fields: `additional_directories`,
    `agents`, `analytics_disabled`, `apiKeySource`, `capabilities`, `fast_mode_disabled_reason`, `fast_mode_state`,
    `memory_paths`, `messaging_socket_path`, `original_cwd`, `output_style`, `per_turn_effort_active`, `plugins`,
    `product_feedback_disabled`, `scratchpad_path`, `skills`, `slash_commands`, `startup_timing` (which carried the
    entrypoint name), `terminal_slash_commands`, `ultrathink_available`, `ultrathink_typed_raises`, `view_mode`.
14. `rate_limit_event.rate_limit_info` is reduced to `status`, `rateLimitType` and `resetsAt`. Removed:
    `isUsingOverage`, `overageDisabledReason`, `overageEnabled`, `overageStatus`, `surpassedThreshold`,
    `unifiedWindows`, `utilization` (plan and credit state of the account).
15. `result` drops account and internal timing detail: `credential_expires_at`, `fast_mode_state`,
    `fast_mode_disabled_reason`, `first_content_frame_ms`, `input_attachments_detail`,
    `message_normalization_detail`, `request_build_detail`, `time_to_request_cpu_ms`,
    `time_to_request_loop_lag_ms`, `time_to_request_major_faults`, `time_to_request_ms`,
    `time_to_request_phases_ms`, `tool_schema_build_detail`, `ttft_ms`, `ttft_stream_ms`, `turn_start_resume_kind`.
16. `result.modelUsage.*` and `result.model_usage_since_last_result.*` drop `provider` and `costBasis`.
17. Every `usage` object in any line drops `inference_geo` and `fallback_credit`.
18. `system/init.tools` is reduced to a neutral subset of standard built-in tool names, in recorded order: `Task`,
    `Agent`, `Bash`, `BashOutput`, `KillShell`, `Edit`, `MultiEdit`, `NotebookEdit`, `NotebookRead`, `Read`, `Write`,
    `Glob`, `Grep`, `LS`, `TodoWrite`, `WebFetch`, `WebSearch` where present. Removed as account- or
    experiment-specific: `Artifact`, `ArtifactComments`, `ArtifactData`, `CronCreate`, `CronDelete`, `CronList`,
    `DesignSync`, `EnterWorktree`, `ExitWorktree`, `ListAgents`, `Monitor`, `PushNotification`, `RemoteTrigger`,
    `ReportFindings`, `ScheduleWakeup`, `SendMessage`, `Skill`, `TaskStop`, `ToolSearch`, `Workflow`.

`run.json.argv` keeps the recorded flags. `--mcp-config` carries `{"mcpServers":{}}` (empty and neutral) and
`--max-budget-usd` is kept as a test parameter. No host name, interpreter path or home-directory path is recorded.

## Retention table (all 177 lines audited)

The audit was a key-path census over every line plus a text search of the whole bundle for home-directory,
host, account, plugin, skill, socket, memory-path, entrypoint, overage and credential terms. None remains.

| Line type | Fields kept | Fields removed (rule) |
| --- | --- | --- |
| `system/init` | allowlist of rule 13; `tools` is the neutral built-in subset of rule 18; `mcp_servers` is empty | everything else (13) |
| `system/status` | `status`, `session_id`, `uuid` | none |
| `system/permission_denied` | `tool_name`, `tool_use_id`, `message`, `decision_reason`, `decision_reason_type`, ids | none; paths are rewritten (1) |
| `system/post_turn_summary` | `status_category`, `status_detail`, `needs_action`, `summarizes_uuid`, ids | none |
| `system/task_summary` | `detail`, ids | none |
| `system/thinking_tokens` | counters (zeroed), ids | none |
| `stream_event` | the whole event including `message_start.message`, `content_block_*`, `message_delta`, `ttft_ms`, `thinking_display`, `api_message_id` | `usage.inference_geo`, `usage.fallback_credit` (17) |
| `assistant` | `message` (content, usage), `request_id`, `timestamp`, `error`, `api_error_status`, `wire_tool_inputs`, ids | `usage.inference_geo`, `usage.fallback_credit` (17) |
| `user` | `message.content` (text and `tool_result`), `tool_result_meta`, `tool_use_result`, `parent_tool_use_id`, `timestamp`, ids | none |
| `rate_limit_event` | `rate_limit_info.status`, `.rateLimitType`, `.resetsAt`, `uuid`, `session_id` | account and overage detail (14) |
| `result` | `subtype`, `is_error`, `result`, `errors`, `terminal_reason`, `stop_reason`, `num_turns`, `result_index`, `queued_turn_count`, `process_turn_index`, `permission_denials`, `total_cost_usd`, `duration_*`, `usage`, `modelUsage`, `model_usage_since_last_result`, `subagent_stats`, `safety_stops`, `api_error_status`, ids | credential, fast-mode, timing and per-phase detail (15), `provider` and `costBasis` (16), `usage` fields (17) |
| `stderr.txt` | the single `r08` line: `[claude-code:unrecognized_model] {"model":"no-such-model-xyz","query_source":"sdk"}` | none |
| `run.json` | `name`, `argv`, `cwd`, `exit_code`, `signals`, `t_ms` | environment, notes, timings of the harness |

`cwd` and tool paths are the recorded work directories (`/workspace/recordings/work/<run>` and
`/workspace/recordings/work/outside/other.txt`). The fake replaces the recorded `cwd` and the recorded session
identifier with the live values when it replays; the `outside` path stays as recorded.

## What is recorded and what is scripted

Recorded (replayed from this bundle, protocol shape only): a one-shot success, a live success, a two-turn live run,
a resume, a tool call, a permission refusal, an unknown-model failure (`subtype: success` with `is_error: true`,
exit 1) and the SIGINT interrupt. Scripted by the fake and not measured on the real CLI: any exit code other than
the recorded ones (`nonzero`), holding a turn open until a gate file exists (`hold`), an escaped descendant
(`escape`), death by SIGTERM, the wording of refused argument vectors, and every bound the fake enforces.

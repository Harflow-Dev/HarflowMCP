# Observed task outcomes

`get_task_status` returns `outcome` after an owning server observes process exit and collects its
output. Outcomes are persisted; recovering status returns the original observation rather than
probing today's repository and presenting it as the task's terminal state. Older records and runs
whose owner could not inspect the result return `null`.

Git observation schema version 2 adds `git_observation_version: 2` and a bounded `repositories`
list containing up to eight distinct worktree roots. The public ctl envelope remains wire `v: 1`;
the observation version describes this additive outcome schema, not transport compatibility.
Legacy top-level fields continue to describe the launch directory, including when it is a non-Git
parent directory. Inspect `repositories` for work performed in child repositories.

Pass `observe_repos` to `start_task` as up to seven additional absolute paths to capture their
starting state before launch. Saved workflow agent candidates (including fallbacks) accept the
same setting; resumes and continuations retain it. These paths are observations only: they do not
grant filesystem, commit or publication authority. The primary directory plus explicit targets
share one starting-observation budget. Canonical worktree roots are deduplicated; linked worktrees
remain distinct and retain Git/common-directory identity evidence.

During a run, normalized structured command directories (`cwd`/`workdir`), literal `git -C`
commands and edit paths can discover additional repositories. Shell interpolation and agent
narration are not repository discovery evidence. A repository first discovered midrun receives a
baseline labelled `first_seen_midrun`; it cannot establish its dispatch state. Repository bounds,
truncation and unavailable discovery evidence remain explicit.

Each repository records `baseline` and `terminal` HEAD, branch and working-tree observations.
`dispatch_head` retains the original observed HEAD. `base_commit`, `head_commit`, `new_commits`
(at most 100, with a truncation flag), `base_provenance` and `committed_diff_stat` describe the
observed comparison range. After a branch switch, an unambiguous locally recorded default-branch reference is preferred,
with the local upstream as fallback, to select a merge-base; the provenance names the comparison. The original baseline is
retained. Legacy outcomes keep their original initial-HEAD range semantics.

Commit ranges contain newly reachable history, including imported commits. They never identify
task-created commits or authorship, even when a task actually committed. HEAD and branch are
endpoint observations; intermediate transitions remain unobserved. Brief status includes bounded
repository counts and endpoint summaries. Full reads retain detailed observations. No reflog or
narration establishes authorship. Missing Git, non-Git targets, oversized output and timeouts
produce unavailable evidence without affecting task execution status.

Baseline and terminal observations add `observation_status`: `success` when HEAD, repository root
and working-tree status are available, `partial` when some are available, or `unavailable`.
Detached HEAD and unborn repositories can legitimately be partial. Each observation's diagnostics
names its phase, shared budget, elapsed time and at most 16 probes. All terminal repository/range
probes share an aggregate three-second deadline; one repository cannot multiply the budget by the
number of targets. Starting observations share 1.5 seconds.

Each probe has a bounded semantic command label, `elapsed_ms` and a status: `success`,
`git_failure` (nonzero exit), `time_limit`, `output_limit`, `skipped_budget_exhausted` (no command
started), or `error` (spawn/metadata setup). Optional return codes, byte counts and bounded error
categories identify the failure without retaining stderr contents. No Git subprocess starts once
its shared deadline has expired. The 256-KiB stdout/stderr ceiling does not limit the packed-refs
input database: Git consumes large refs through the same isolated directory under its time budget.

The outer starting-observation guard freezes available partial fields rather than replacing the
entire baseline with null. Its diagnostics add `outer_status` (`outer_timeout` or `worker_error`)
and `outer_budget_ms`. It remains a best-effort observation: filesystem setup/cleanup and OS process
creation are not a hard real-time guarantee. An unavailable field does not invalidate a separately
observed commit or diffstat. Their provenance remains local Git, while normalized tool evidence
has its separate provenance; agent narration is never promoted to independently observed Git.
Inspect the affected phase/probe to distinguish command failure from skipped work. For time/output
limits, reduce unrelated checkout activity or inspect repository status separately; Harflow keeps
its bounds and does not infer the missing result. Optional observation failures never change the
task's completion status.

[Task artifact registrations](task-artifacts.md) are separate metadata references.
A task may register a report and later fail; neither registration nor current path
availability establishes successful execution or content validity.

Git commands have time and output budgets and never contact a remote. They ignore ambient
repository selectors and injected Git configuration so observations remain scoped to the task
repository. Working tree status reads through a disposable Git directory with repository, global
and system configuration excluded;
clean/process filters, hooks and filesystem monitors cannot execute in the server. Status therefore
describes raw working tree contents: repositories depending on content normalization filters or
custom Git configuration may appear dirty even when their ordinary Git status appears clean.
Pre-existing changes remain visible in the baseline; terminal differences cannot establish task
authorship in a shared repository. Untracked directories use Git's normal collapsed presentation.

`local_upstream` and `local_remote_refs` describe locally cached tracking refs, containment and
ahead/behind counts. Their freshness is unknown: matching local refs cannot prove current remote
state. `publication_evidence` retains bounded correlated standalone push/PR command results,
including failures, command, call ID, exit code and repository identity. A successful push command
is evidence of that command's result, never proof of current remote contents or exclusive task
authorship. `push_evidence` names the local-ref corroboration and its unknown freshness; `pushed` describes
that historical local evidence, never a current remote guarantee or attribution to a successful
command. Failed command results remain failures. Unavailable publication stays `null`.

At publish-capable dispatch, a non-Git launch directory receives a neutral notice without advice
to cancel. Explicit observation targets are checked separately. A real repository's unknown
branch notice identifies the observed cause (for example detached HEAD, no local remote HEAD,
missing Git or timeout). Default branches come only from locally recorded remote HEADs; this
check never fetches or guesses `main`/`master`. It samples once before launch, so it does not
establish the eventual publication branch.

`last_build_test` records the last recognized build/test command from correlated normalized tool
calls/results, its observed exit code (or `null` if the harness did not supply one), and call ID.
It is best effort and does not discover every custom script or prove complete verification.
Recognition covers executable starts in simple shell command lists; unsupported shell expansions
and control grammar remain unavailable. A command list can contain a build/test invocation that
was never reached. `exit_code_scope` therefore identifies the entire tool invocation, not an
individual test segment. Retained strings are limited to 8192 UTF-8 bytes; tool shortening is
explicitly flagged, and oversized commands are unavailable for recognition. Oversized paths are
discarded rather than shortened into external-write evidence. Oversized correlation IDs use an
explicit SHA-256 encoding; ordinary IDs remain verbatim, preserving event-log references without
retaining arbitrarily large IDs. At most 100 pending calls are retained.
`external_writes` records at most 50 successful normalized edit results whose resolved paths lie
outside the repository. Shell commands and missing paths cannot establish external-file writes;
`null` means unavailable, not proof that no external writes happened. Each observation includes
provenance. Neither Git observations nor tool results establish that the requested goal succeeded.

## Permission denial classification

`permission_denials` entries on a task snapshot, and the task-level `denial_categories` list (the
distinct categories in first-seen order), say why a harness refused an action. Every dict entry
carries `denial_category`, `denial_remedy` and `classification_basis`. All three are additive and
optional: records written earlier simply lack them, and non-dict entries are returned untouched.

Claude Code classifies for versions measured in the shared qualification table (2.1.310); Codex classifies only
from evidence Harflow holds (below); other backends are `unclassified` with `classification_basis.reason: "backend_unmeasured"`. An entry is classified only when its `tool_use_id` appears in a result event's
`permission_denials` and the matching error `tool_result` text equals a measured text exactly.
An entry seen only in the stream is `unclassified` with `classification_basis.reason: "stream_only"`,
because an agent's own command output can print a refusal sentence. Other reasons are
`unknown_version` (no init version, or a denial before it), `unmeasured_version` (including a run that
spanned several versions), `no_refusal_text`, `unrecognized_text` and, for other backends,
`backend_unmeasured`. Unclassified entries carry no inferred remedy.

| `denial_category` | Meaning and remedy |
| --- | --- |
| `deny_rule` | A deny rule refused it (Harflow's access level, user or project settings, or managed policy). Allow rules cannot override it. `harflow_access_deny: true` means Harflow also supplied a matching deny prefix for the run; it does not identify the unique cause of refusal. |
| `approval_required` | Approval was requested. A matching allow rule or caller approval may help, but inherited ask or deny rules can remain authoritative; `allowed_commands` does not necessarily override them. The refusal does not identify its source or rule. The generic Bash permission-request sentence is classified only on measured Claude 2.1.310. |
| `chained_command` | One part of a multi-operation command needs approval (`part`); run each part separately. |
| `unverifiable_command` | Command substitution that cannot be checked before it runs; avoid it. |
| `path_safety` | A built-in path check allow rules cannot satisfy; caller approvals can approve it on qualified versions. |
| `outside_working_directory` | A write outside the repository that was not granted. On a qualified version a caller can approve an out-of-repo Write or Edit one exact action at a time when the task has `caller_write_approvals`. |
| `caller_rejected` | The caller rejected the exact action. |
| `harflow_policy_rejected` | Harflow's own policy refused it (`policy_reason`: `write_approvals_not_enabled`, `protected_path`, `request_too_large`, `approval_expired` or `path_changed_since_admission`, where the approved action's paths changed or became protected before delivery and the caller's recorded `approve` was converted to a Harflow rejection); never a caller decision. |
| `policy_denied` | Codex: an exec-policy rule with a `forbidden` decision refused the command (`justification` is the rule's). Seen on the app-server's stderr only while a turn is active, in the one measured format; approvals cannot override it. |
| `approval_policy_never` | Codex exec path: the run could not ask for an escalation (approval policy never). Start the task with `caller_approvals` (macOS, write_in_repo or publish) or use a freedom that needs none. |
| `unclassified` | Inspect the refusal text. |

Codex with `caller_approvals` classifies a declined command or file change from the answer Harflow wrote for its exact ledger identity (`caller_rejected`,
or `harflow_policy_rejected` with the `policy_reason` of the system rejection or expiry); a decline Harflow did not write, and a replayed log, are `unclassified`
(`no_harflow_answer`, `no_ledger_evidence`). An MCP tool call rejected with `user rejected MCP tool call` is `harflow_policy_rejected` (`mcp_approval_unsupported`) only when
exactly one elicitation Harflow answered with an error in the same thread and turn matches its server, tool and (non-null) arguments; otherwise `no_matching_elicitation` or `mcp_association_unclear`. Sandbox
denials (`Operation not permitted`) and anything seen only in command output are never classified.

The `tool_result` event for the refused call carries no category, because the result event that
authorizes it has not arrived yet. The `task_finished` event adds `denial_categories` and the
bounded projection of the classified `permission_denials` when there are any (at most 32 entries within a small byte budget, without tool input, plus `denials_omitted`); snapshots and workflow evidence keep the full list. Retry and continuation gating still looks only at
whether denials exist.

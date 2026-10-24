# Observed task outcomes

`get_task_status` returns `outcome` after an owning server observes process exit and collects its
output. Outcomes are persisted; recovering status returns the original observation rather than
probing today's repository and presenting it as the task's terminal state. Older records and runs
whose owner could not inspect the result return `null`.

The outcome records a starting `baseline` and `terminal` observation: HEAD, branch and working tree
status. `base_commit`, `head_commit`, `new_commits` (at most 100, with a truncation flag) and
`committed_diff_stat` describe the committed range. A missing/non-Git repository, absent Git,
oversized output or timeout produces explicit unavailable fields. Failures never affect the
agent's execution status.

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

`local_upstream` describes only the locally cached tracking ref and ahead/behind counts. `pushed`
and `publication_evidence` are unavailable (`null`); a tracking ref cannot prove publication.

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

Only Claude Code classifies, and only for versions measured in the shared qualification table
(2.1.310). An entry is classified only when its `tool_use_id` appears in a result event's
`permission_denials` and the matching error `tool_result` text equals a measured text exactly.
An entry seen only in the stream is `unclassified` with `classification_basis.reason: "stream_only"`,
because an agent's own command output can print a refusal sentence. Other reasons are
`unknown_version` (no init version, or a denial before it), `unmeasured_version` (including a run that
spanned several versions), `no_refusal_text`, `unrecognized_text` and, for other backends,
`backend_unmeasured`. Unclassified entries carry no inferred remedy.

| `denial_category` | Meaning and remedy |
| --- | --- |
| `deny_rule` | A deny rule refused it (Harflow's access level, user or project settings, or managed policy). Allow rules cannot override it. `harflow_access_deny: true` is added only when the command matches Harflow's own deny prefixes for the run's freedom. |
| `approval_required` | No matching allow rule: a matching allow rule, `allowed_commands` or caller approvals are the ways forward. |
| `chained_command` | One part of a multi-operation command needs approval (`part`); run each part separately. |
| `unverifiable_command` | Command substitution that cannot be checked before it runs; avoid it. |
| `path_safety` | A built-in path check allow rules cannot satisfy. |
| `outside_working_directory` | A write outside the repository that was not granted. |
| `caller_rejected` | The caller rejected the exact action. |
| `harflow_policy_rejected` | Harflow's own policy refused it (`policy_reason`); never a caller decision. |
| `unclassified` | Inspect the refusal text. |

The `tool_result` event for the refused call carries no category, because the result event that
authorizes it has not arrived yet. The `task_finished` event adds `denial_categories` and the
classified `permission_denials` when there are any. Retry and continuation gating still looks only at
whether denials exist.

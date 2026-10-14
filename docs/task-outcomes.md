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

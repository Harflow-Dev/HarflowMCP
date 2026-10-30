# Recent usage-limit hits

`recent_limit_hits` (MCP) and `harflow-ctl limit-hits --json` answer one question: which backend and
model combinations were recently rejected by a provider usage limit, and when does the provider say
the limit resets. They read persisted task records only, including the tasks that workflow nodes
ran. Nothing is retried, switched, continued or scheduled, and no task log is read. Attempts with no
task record (Codex-native workflow attempts) and workflow recovery state are covered by the
[workflow view](#workflow-usage-limits) below.

Capability `limit-hits.v1` on the fresh `harflow-ctl version-info --json` handshake qualifies
`limit-hits`. Wire shapes and fixtures are in [public contracts](contracts.md#recent-usage-limit-hits).

## Evidence

A hit exists for a task only when its persisted record carries `failure_diagnostic` with
`category: usage_limit`. A `provider_error` diagnostic, an ordinary failure and any log or prose text
are never evidence. Records written before diagnostics existed have none and contribute nothing.
Hits are indexed in a `task_limit_hits` side table of the disposable listing catalog, which is
updated whenever a task record is written and rebuilt through bounded bootstrap. A record that
later loses the diagnostic, or is deleted, loses its hit.

`observed_at` is when the task settled: the record's `finished_at`, else its `started_at`, and
`observed_at_source` names which. It is not necessarily when the provider rejected the request,
and a recovered record that settled later can sort later. A record with neither time cannot be
ordered and has no hit. `requested_model` is the model the task was dispatched with; `null` means
the backend default, and a different model the harness actually used is not reported here.

## Query

The latest hit per (backend, requested model) is returned, newest first, for at most 50 pairs. There
is no pagination. A query examines the newest 500 hit rows (by `observed_at`, ties broken by task id)
that match its indexed bounds: `since`, `backend` and `model`. An unfiltered query therefore sees only
the newest 500 hits overall, while filtering by `backend`, or by `backend` and `model`, gives that
backend or pair its own newest-500 window, so a rare combination behind a retry storm of another
backend is still reached. `repo` and `group` filter inside the examined window.

| Filter | Meaning |
| --- | --- |
| `since` | Inclusive ISO8601 timestamp with time zone; bounds the window. |
| `backend` | A registered backend name; bounds the window. |
| `model` | The requested model, exactly; bounds the window. Tasks on the backend default have no model to match. |
| `repo` | The task's repository path, exactly; filters inside the window. |
| `group` | The task's group, exactly (at most 128 characters); filters inside the window. |

```json
{"hits": [{"backend": "codex", "requested_model": "gpt-x", "task_id": "…", "title": "…", "group": null,
           "observed_at": "2026-10-27T01:00:00+00:00", "observed_at_source": "finished_at",
           "diagnostic": {"category": "usage_limit", "source": "stderr", "reason": "…"},
           "reset": {"raw": "2099-01-01T00:00:00Z", "at": "2099-01-01T00:00:00+00:00", "state": "future"}}],
 "scan": {"examined": 4, "limit": 500, "complete": true},
 "index_state": {"status": "ready", "source": "task_catalog", "pending_records": 0, "blocked_records": 0}}
```

`scan.complete` is `false` when older matching hits were left outside the 500-row window, when more than 50
pairs matched, or when `index_state.status` is not `ready`. While the index is `preparing`, each call
indexes a bounded batch of records and returns the hits found so far; retry until it is `ready`. A
`blocked` index has records that need direct inspection, and their hits are missing from the answer.

## Reset time

`reset.state` is judged when the question is asked:

- `future`: the stated reset time is later than now.
- `elapsed`: the stated reset time has passed. This says only that the time passed. It does not say
  the provider is available again, because a later task may have been rejected again; check for a
  newer hit and expect to try again.
- `unknown`: no reset was stated, or the stated value could not be used. A value that fails
  validation (a malformed string, a string without an offset, a non-finite number) omits `raw` and
  `at`. A value that validates but cannot be represented as a date (a number far outside the
  calendar) keeps `raw` and omits `at`.

`raw` is the provider-supplied value exactly as persisted. A number is epoch seconds; a string is an
ISO8601 timestamp that carries its offset. `at` is the same instant in UTC.

## Who can read it

Only a direct caller. A workflow-managed task, any other verified agent caller and a caller whose
identity cannot be established are refused before the catalog is opened. The MCP tool answers
`invalid_params`; `harflow-ctl limit-hits` answers `agent_caller` or `caller_undecidable`, like the
other direct-caller commands. An unreadable catalog answers `limit_hits_error` on the command line.

## Workflow usage limits

`workflow_limit_hits` (MCP) and `harflow-ctl workflow-limit-hits --json` report the same kind of
evidence for workflow runs, from the persisted run documents rather than task records: which attempts
were rejected by a usage limit, which runs are blocked waiting for a recovery decision, and where to
inspect each. Nothing is retried, switched, continued or scheduled, and no task log is read. Capability
`workflow-limit-hits.v1` qualifies the command. Wire shapes and fixtures are in
[public contracts](contracts.md#workflow-usage-limit-hits).

### Evidence

A hit exists for an attempt only when the run document holds `activations[].tasks[].result.failure_diagnostic`
with `category: usage_limit` for it. That covers headless attempts and Codex-native children alike, including
attempts that never had a task record. `node_result`, the `evidence[]` copies of a result, the
native update copies and the text of `usage_recovery_history` are never read; the history only adds
`recovery {accepted, answer}` to the hit its accepted answer responded to (the same backend, requested model
and diagnostic). A `provider_error` diagnostic and any prose are not evidence.

Every attempt is its own hit, keyed by its task id within its run: a retry, a resumed attempt and each fallback
candidate are separate hits, and so is every native child, even when several children share one transport. A
child's hit carries `transport_task_id`, so a reader can group children that ran under the same transport. The run
document records no provenance that marks a child's diagnostic as a copy of its transport's rejection, so nothing is
merged. A child run's hits belong to the child run; a parent never reports them again. A run with more than 100
hits keeps its newest 100 plus the newest hit of up to 20 further (backend, requested model, observed model) keys,
at most 120 rows, so the latest hit per key stays correct unless a run has more keys than that. Such a run is
recorded once in a small per-run table. An answer reports `scan.complete: false` when a truncated run in its scope
could change it: whenever `since` or `before` is set (older dropped hits may fall inside the range), and without
them only when a run also dropped a whole key. A direct query checks the same scope as its hit window, with an
indexed lookup that stops at one row, so the check does not grow with history. `observed_at` is when the attempt settled (its `finished_at`, else the first usable of
`started_at`, the execution's `finished_at` or `created_at`, or the run's `updated_at`, named by
`observed_at_source`); it is not necessarily when the provider rejected the request. `requested_model` is the
model the candidate asked for (null means the backend default) and `observed_model` the model the harness
reports having used.

A run is blocked when it holds `orchestrator_usage_recovery` or `worker_usage_recovery` and is not finished: a
`completed`, `failed`, `cancelled` or `cancelling` run is not listed, since cancelling never clears the checkpoint.
Otherwise the stored status does not matter. It is listed under `current` on the run that owns the checkpoint, not on its root: a root that is
`running` while a child waits for a decision reports the child. `detail_ref` names the
`get_workflow_run_detail` call (`view: "execution:<id>"`) that returns the execution; a managed orchestrator
reads settled executions with `inspect_workflow_node` instead. A run document that cannot be read or fails
validation, including a malformed `reset_at`, is listed under `unavailable` (up to 20) and the answer is not
complete; it is not reported as an unknown reset.

### Query

`latest` holds the newest hit per (backend, requested model, observed model), at most 50 keys, and `current`
holds at most 100 blocked runs. A direct query examines the newest 500 hit rows (by observed time, ties broken
by run id and task id) that satisfy one indexed bound: `root_workflow_run_id` if set, else `workflow_id`, else
`repo`, together with the `since` (inclusive) and `before` (exclusive) range. The remaining filters apply inside
that window, so a rare workflow, repo or tree is reachable behind a retry storm of another when it is the bounding
filter. There is no pagination. `--current-only` skips the hit window.

| Filter | Meaning |
| --- | --- |
| `root_workflow_run_id` | Runs of this workflow tree (an id prefix of at least 8 characters is accepted); bounds the window. |
| `workflow_id` | The stable workflow identity; bounds the window unless a root is given. |
| `repo` | The run's exact recorded repository path; bounds the window unless a root or workflow is given. |
| `since`, `before` | Inclusive and exclusive ISO8601 timestamps with a time zone on `observed_at`; they bound hits only, not `current`. |
| `current_only` | Report only blocked runs. |

### Who can read it

A direct caller reads every run. A workflow orchestrator reads its own run and its descendants. Candidates are the
children named by the persisted invocations of the caller's own verified runs (at most 200 are examined), each
verified by its single edge to an already-verified parent (its `parent_link` names that parent and invoking
execution, its root is the caller's tree root, and the parent's pinned dependency tree holds the edge). Each child
is read once with the catalog's per-document byte bound; an oversized or unreadable one is listed under
`unavailable`. Unverified links never count toward the 65-member cap, which applies to verified members only, and a
named child that fails verification, like reaching either limit, makes the answer incomplete. A
named child that is not yet indexed is indexed first; while it stays unresolved the answer is a `preparing`
response with no hits, never a complete one. Each run contributes at most 120 rows, so a managed query examines at
most 65 x 120 rows.
it never sees ancestors, siblings or unrelated runs. Every count and flag covers authorized runs only, and
unreadable or pending runs elsewhere in the catalog do not affect the answer; an unreadable descendant named by
a verified parent's invocation is listed under `unavailable`. `root_workflow_run_id` is refused for managed callers
(results are already scoped to the caller's tree). Workflow workers and builders and callers whose identity cannot be established are refused
before any run is looked up. An unreadable catalog answers `workflow_limit_hits_error` on the command line.

## Related

- [Usage-limit recovery in workflows](workflows.md#usage-limit-recovery)
- [Provider failure diagnostics](contracts.md#provider-failure-diagnostics)

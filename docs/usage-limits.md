# Recent usage-limit hits

`recent_limit_hits` (MCP) and `harflow-ctl limit-hits --json` answer one question: which backend and
model combinations were recently rejected by a provider usage limit, and when does the provider say
the limit resets. They read persisted task records only, including the tasks that workflow nodes
ran. Nothing is retried, switched, continued or scheduled, and no task log is read. Attempts with no
task record (Codex-native workflow attempts) and workflow recovery state are not covered by this view.

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

## Related

- [Usage-limit recovery in workflows](workflows.md#usage-limit-recovery)
- [Provider failure diagnostics](contracts.md#provider-failure-diagnostics)

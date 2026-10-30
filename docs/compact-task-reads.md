# Compact task reads

Full task status, events, and the existing `harflow-ctl list` / `status` remain
available. Compact reads are explicit display projections; they never rewrite
prompts, follow-ups, messages, summaries, or any other stored caller prose.

MCP `get_task_status` and `wait_for_task` default to `detail="compact"`: identity,
status and reconciliation evidence, the complete summary/report, an outcome headline,
up to three recent activity lines, approval/failure/recovery evidence and references to
full status and event reads. `get_task_status(..., brief=True)` is a compatibility alias
for this projection. `detail="full"` restores static execution metadata and detailed
outcomes; `include_tail=True` explicitly adds the raw tail in either mode. Complete reports
are not subject to a display-size cap; see [waiting and polling](multi-task-waiting.md).

The separate ctl option `harflow-ctl status TASK --brief --json` retains its existing
at most 8 KiB display projection with bounded summary/outcome and `truncated_fields`.
It does not select the MCP projection. Outcome publication values remain nullable:
absence of publication evidence does not mean a push failed or succeeded.
Standalone artifact registrations add a bounded `artifact_summary` with a count,
availability of registration metadata, and a detail reference. Full notes and current
file observations are paged separately through [task artifact reads](task-artifacts.md).
The ctl brief projection bounds response size; obtaining a recovered status can still
require the existing recovery/replay work before projection.

For indexed history use `harflow-ctl task-list-compact --json --limit 50`, or
explicit bounded listing options on the MCP `list_tasks` tool. These compact
history listings are available only to direct callers. Once caller readiness is
established, managed workflow callers receive an invalid-parameters error before
any global compact history query. While caller readiness is preparing, the CLI
retains its catalog preparation response; the MCP tool reports incomplete caller
authority as an invalid-parameters error.

Managed callers can inspect explicit known IDs with
`harflow-ctl task-list-page --json --task-ids TASK1,TASK2` (MCP
`list_task_page(task_ids=["TASK1", "TASK2"])`), subject to their existing task
visibility, or use status reads for known tasks. Compact filters are ANDed:

- `--group`, `--repo`, `--status`, `--backend`: exact indexed header values.
- `--title-contains`: case-insensitive substring (SQLite's ASCII case folding).
- `--since`: inclusive start timestamp, ISO8601 with timezone, for example
  `2026-10-14T00:00:00Z`. This differs from the legacy `list --since 7d`.
- `--fields task_id,title,status`: requested compact fields. Identity and status
  uncertainty fields are always retained. Unknown fields and filters are errors that name
  the bad entries, suggest a close match (`repo` means `repo_path`), and list the accepted
  names. An empty field list, a non-string entry and a list longer than the accepted set
  each have their own message. `harflow-ctl task-list-compact --fields` uses the same checks.

Limits are 1–100, default 50. The group filter accepts nonblank strings of at
most 128 characters, matching task creation (including Unicode). Other filter
strings are nonempty and at most 200 UTF-8 bytes. Responses retain catalog readiness, incomplete-history and reconciliation
metadata. They omit global active counts, which would be misleading for a filtered
page. Status filters are indexed best-effort reads: a process can end without
changing its metadata, and identity reconciliation runs after indexed selection.
Each request checks up to 100 selected identities plus up to 100 rotating active
identities. If rotating checks change a status or active identities remain
unchecked, `status_reconciliation_pending=true`, `history_incomplete=true`, and
`counts_complete=false`; `polling_guidance` directs a retry with the same filters
**without a cursor**. Newly matching rows may precede the old cursor. An empty
page with `has_more=false` does not establish absence while reconciliation is
pending. When reconciliation is settled, follow `next_cursor` while `has_more`
is true. Large active inventories can remain pending across retries because
every request has a bounded identity budget.

Rows sort newest first by immutable catalog start time, then descending task ID.
Pass the returned opaque `next_cursor` with the same filters. It is rejected in
a different directory or filter scope. Changing fields or page size is allowed.
New arrivals before the cursor do not shift the remaining history. This is keyset
pagination, not a frozen snapshot of concurrent edits.

Filters run in the durable catalog before task record materialization or event
log reads. Group matches the exact creation label in a separate bounded index.
Title and repository filters apply to bounded display headers (normally at most
200 characters); unusually long legacy values can be truncated or omitted there. Use full status for those task IDs rather than
assuming a filtered miss proves absence. Oversized or unsupported historical
records retain direct-lookup and incomplete-history markers. Catalog bootstrap
is bounded and may require repeated reads before results are ready.

A reproducible test creates 200 settled tasks and 20 MB of raw logs, then forbids
metadata materialization while requesting five filtered rows:

```sh
UV_CACHE_DIR=/tmp/harflow_mcp-uv-cache uv run pytest -q -s tests/test_task_reads.py::test_reproducible_large_history
```

The warm-catalog run measured a 2,586-byte response in 0.008182 seconds on the
implementation machine, with zero metadata reads and zero log reads. This is a
local reproducibility sample, not a latency guarantee or a cold-bootstrap claim.

Group matching uses a separate bounded index of the exact label, even when the
displayed header is shortened. The v11 derivative catalog rebuilds existing history
through the usual bounded bootstrap; retry while `bootstrap_pending` is true.

## Batched full status reads

`--task-ids` entries may be unique id prefixes of at least 8 characters. The reply keeps one row per
input, each row from a prefix carries `resolved_from`, and `errors` stays keyed by the input you sent
(`unknown_task` with `data.reason`, or `ambiguous_task_id` with `data.candidates`; see "Id prefixes"
in `contracts.md`).

`harflow-ctl status-batch --task-ids TASK1,TASK2 --json` returns several full
snapshots in one document and one process:
`{"v": 11, "tasks": [<snapshot>...], "errors": {"<id>": {"code", "message"}}}`.
It is an additive command under `CTL_JSON_VERSION` 11 — no existing document
changes shape. Entries are exactly what `status <id> --json` reports (only a
running task's `duration_seconds`, computed per call, may differ), and `errors`
carries the same codes single `status` reports: `unknown_task`, read-refusal
codes, and `invalid_task_id`. A whole-batch refusal — including undecidable
caller authority — is the `status` error shape for the whole command, never
partial data. At most 100 exact comma-separated ids per call; duplicates are
collapsed. There is no `--brief` variant.

The command reads caller metadata once for the whole batch (the same one-scan
caller detection `status` and the MCP workflow reads use) and, like every other
ctl read, never starts retention, never starts the notification worker, and
never constructs a registry. The workflow node reads (`workflow inspect`,
`workflow assigned-input`) share the MCP tools' checks through
`workflow_node_reads` and never import the MCP server module; the ctl paths
that do need registry-backed server helpers (`permission-preview`, mutating
workflow actions) install a registry without the maintenance sweep or the
notification worker. See the Monitor contract notes in CLAUDE.md.

## Retained-history metadata

Workflow history headers and compact status may include nullable numeric `finished_at`
(Unix seconds), `workflow_id` and, once the caller has resumed the run, numeric
`resumed_at` (Unix seconds; absent otherwise, and unaffected by node or engine activity).
New runs start with `finished_at: null`. The engine
stamps one observed time on a nonterminal-to-terminal status transition (`completed`,
`failed`, `cancelled`, or `timed_out`), using that same time for `updated_at`. Reopening
a terminal run clears completion time; a subsequent terminal transition records a new
time. Private `finished_at_sequence` is a nonnegative integer witness matching the run's
exact integer `sequence`. A completion date is qualified only for a terminal run whose
finite numeric timestamp has a matching witness. Root and child runs initialize null
completion with witness zero. Compatible older writers advance sequence without advancing
the witness, so their writes conservatively invalidate completion evidence. Reads expose
unknown without changing authoritative files. New terminal bookkeeping preserves only
previously qualified evidence; it never certifies a stale timestamp by advancing its
witness. Only a newly observed terminal transition restores a known completion date. `updated_at` is never backfilled as a completion date. Completion time is
independent of `settling`; consumers counting finished work must also require false
settlement state.

Workflow identity comes only from the run's pinned definition, never the current saved
workflow with the same name. Modern IDs distinguish deleted and recreated definitions.
Historical definitions without an ID use the existing deterministic name-hash identity;
two legacy generations with the same name can therefore be ambiguous. Builders always
omit projected workflow identity because their descriptor is not a saved definition. Consumers can exclude builders and
nested runs using `kind` and the parent/root discriminators.

Task page `workflow_run_id: null` means an ownership receipt was conclusively absent
at observation time. A valid nonempty string identifies its authoritative workflow owner.
Missing ownership metadata does not mean standalone: older engines, malformed receipts,
and unreadable receipts cannot establish that classification. Receipt uncertainty marks
history and counts incomplete and retains structured diagnostics when available. These
fields are projections, not a task-record migration or historical ownership inference.
Task `finished_at` retains its existing nullable ISO8601 representation; workflow
`finished_at` is numeric. A missing terminal task completion time is unknown and has no
safe completion-date fallback in task listing metadata.

Summaries describe retained history only, not all-time activity after retention. Require
ready, complete pages, paginate primary items, and exclude related headers from totals.
Do not treat missing timestamps, identity, classification or settlement evidence as known
zero. A legacy terminal workflow's valid `updated_at` strictly before a requested window
can establish only that it is too old for that window; it must never supply its completion
date. Recent, future, invalid or contradictory metadata retains uncertainty.

Task projection version 3 and workflow projection version 3 rebuild existing headers
through bounded bootstrap without rewriting authoritative history. The `.listing.v11.sqlite3`
derivative namespace coexists with older engines' v9 and v10 caches and sidecars, sharing the
existing `.listing.lock`. Recovery removes only the current derivative namespace. Completion, identity,
classification and settlement metadata survive normal 2 KiB header pressure. A header
whose required evidence cannot fit retains a direct-lookup marker and makes the catalog
incomplete instead of publishing truncated evidence. Public wire and persisted run
formats remain additive version 1; older documents may omit every new field.

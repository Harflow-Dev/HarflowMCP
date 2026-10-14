# Compact task reads

Full task status, events, and the existing `polybridge-ctl list` / `status` remain
available. Compact reads are explicit display projections; they never rewrite
prompts, follow-ups, messages, summaries, or any other stored caller prose.

`get_task_status(..., brief=True)` returns an at most 8 KiB JSON projection:
identity, status and reconciliation evidence, title, bounded summary and outcome,
up to three recent activity lines, and references to full status and event reads.
Oversized fields appear in `truncated_fields`. Outcome publication values remain
nullable: absence of publication evidence does not mean a push failed or succeeded.
The same projection is available with `polybridge-ctl status TASK --brief --json`.
The projection bounds response size; obtaining a full recovered status can still
require the existing recovery/replay work before projection.

For indexed history use `polybridge-ctl task-list-compact --json --limit 50`, or
explicit bounded listing options on the MCP `list_tasks` tool. These compact
history listings are available only to direct callers. Once caller readiness is
established, managed workflow callers receive an invalid-parameters error before
any global compact history query. While caller readiness is preparing, the CLI
retains its catalog preparation response; the MCP tool reports incomplete caller
authority as an invalid-parameters error.

Managed callers can inspect explicit known IDs with
`polybridge-ctl task-list-page --json --task-ids TASK1,TASK2` (MCP
`list_task_page(task_ids=["TASK1", "TASK2"])`), subject to their existing task
visibility, or use status reads for known tasks. Compact filters are ANDed:

- `--group`, `--repo`, `--status`, `--backend`: exact indexed header values.
- `--title-contains`: case-insensitive substring (SQLite's ASCII case folding).
- `--since`: inclusive start timestamp, ISO8601 with timezone, for example
  `2026-10-14T00:00:00Z`. This differs from the legacy `list --since 7d`.
- `--fields task_id,title,status`: requested compact fields. Identity and status
  uncertainty fields are always retained. Unknown fields and filters are errors.

Limits are 1–100, default 50; filter strings are nonempty and at most 200 UTF-8
bytes. Responses retain catalog readiness, incomplete-history and reconciliation
metadata. They omit global active counts, which would be misleading for a filtered
page. A process that ends during reconciliation can leave a short or empty page;
follow `next_cursor` while `has_more` is true.

Rows sort newest first by immutable catalog start time, then descending task ID.
Pass the returned opaque `next_cursor` with the same filters. It is rejected in
a different directory or filter scope. Changing fields or page size is allowed.
New arrivals before the cursor do not shift the remaining history. This is keyset
pagination, not a frozen snapshot of concurrent edits.

Filters run in the durable catalog before task record materialization or event
log reads. They apply to its bounded display headers (normally at most 200
characters per title/group/repository); unusually long legacy values can be
truncated or omitted there. Use full status for those task IDs rather than
assuming a filtered miss proves absence. Oversized or unsupported historical
records retain direct-lookup and incomplete-history markers. Catalog bootstrap
is bounded and may require repeated reads before results are ready.

A reproducible test creates 200 settled tasks and 20 MB of raw logs, then forbids
metadata materialization while requesting five filtered rows:

```sh
UV_CACHE_DIR=/tmp/polybridge-uv-cache uv run pytest -q -s tests/test_task_reads.py::test_reproducible_large_history
```

The warm-catalog run measured a 2,586-byte response in 0.008182 seconds on the
implementation machine, with zero metadata reads and zero log reads. This is a
local reproducibility sample, not a latency guarantee or a cold-bootstrap claim.

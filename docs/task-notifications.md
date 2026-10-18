# Task completion notifications

Administrators configure `PB_NOTIFY_DESTINATIONS` in the server environment:

```json
{"builds":{"url":"https://receiver.example/hooks/polybridge","secret_env":"PB_WEBHOOK_SECRET"}}
```

Callers supply only the destination alias (`builds`), never a URL or secret. HTTPS
and globally routable resolved addresses are required by default; each configured
destination URL must be a string, with any explicit port in the range 1–65535. The trusted
administrator can set `allow_private: true` per destination to permit private
addresses and HTTP, including isolated local test receivers. Treat that override
as authority to access the configured internal service. Redirects and proxies are
not used; all DNS addresses are validated before any connection on every attempt.
Numeric addresses are tried in resolver order after TCP connection failures, sharing
one ten-second attempt deadline (including DNS); each connection gets a share of
the remaining time so later addresses can be tried. The connection retains the
original TLS hostname for certificate checks and the HTTP Host header. TLS failures
and failures after HTTP transmission begins do not try another address within the
attempt, avoiding an additional POST to another address.

Subscriptions are durable in `notifications.sqlite3` beside the tasks directory
(mode 0600). A task and alias have one stable subscription ID during receipt retention. Repeating subscribe
returns its existing state, including after unsubscribe or exhaustion. Delivery
states are `pending`, `delivering`, `retry`, `delivered`, `failed`, `expired`, and
`unsubscribed`; delivery state never changes the task's execution status.

The server reconciles raw durable task records. It sends only after a recorded
terminal status, observed exit code, and finish timestamp. An inferred process
death or in-memory completion event does not qualify. `output_drained` is reported
honestly and does not delay delivery. Old records can have a null summary; inspect
the referenced task record. No prompt, raw log tail, secret, or explicit repo_path field is
included. Summaries and Git observations can contain file paths. Outcome observations preserve unknown publication rather than implying
publication succeeded.

Payload version 1 has `event: "task.settled"`, `notification_id`, task ID, title,
group, status, exit code, finish timestamp, output drainage, bounded summary,
outcome projection, and diagnostic task-record reference. Payloads are at most
16 KiB. Receivers must deduplicate using `notification_id`: receipt is not guaranteed after bounded retries, and duplicates are possible
when a receiver accepts a request just before a server crash.

Headers include `X-Polybridge-Notification-ID` and `X-Polybridge-Timestamp`.
When `secret_env` is configured, `X-Polybridge-Signature` is `sha256=` plus the
hex HMAC-SHA256 of `id + "." + timestamp + "." + raw_body`. The secret is read
from the server environment on each attempt and never persisted. Verify the
signature and timestamp freshness, and deduplicate IDs at the receiver.

Only 2xx responses acknowledge delivery. Failures retry with exponential delay,
up to six attempts; errors are deliberately sanitized. Leases allow another
server process to resume abandoned attempts after 60 seconds. The worker scans
at most 16 subscriptions per tick. At most 1000 retained subscriptions exist;
subscriptions expire after 30 days once any active delivery lease has elapsed,
and final states are retained for 30 days.
Pending subscriptions protect their task records from retention. Unsubscribe can
race with an already transmitted request; it cannot recall a request.

Use `start_task(..., notify="builds")` to reserve durable notification intent before
spawning, or `subscribe(task_id, notify="builds")` for an existing task. These
actions, `unsubscribe(task_id, notify="builds")`, and `get_task_notifications(task_id)`
require a direct caller: delegated agents cannot use server HTTP to bypass their
own network or publishing restrictions. Root workflow subscriptions are also supported (see below).
A known rejected start releases its unused reservation. Cancellation while reserving
waits for the reservation commit and removes the unused intent before propagating.
After dispatch begins, cancellation only removes intent when the dispatch explicitly
proves no process started; an uncertain start retains its reservation for up to 30 days. Inspect the returned task identity
before retrying an uncertain dispatch.
Capability refusals retain their specific explanation when dispatch explicitly marks
the failure as occurring before process creation, no task is registered, and its
untouched reservation was removed. Missing task records alone cannot establish that
no process started: persistence can fail after process creation. If cleanup
or dispatch remains uncertain, the error instead includes the reserved task ID
for inspection before retrying.
Status reads use an existing database read-only, without initializing or changing
storage; an absent database returns an empty list and does not start a worker.
Unsubscribing with no notification database also returns an empty list without
initializing storage.

Workers resume when a server opens its task registry if destinations are configured
or a notification database already exists, within a running event loop. Synchronous
CLI commands that open the registry do not start a notification worker. Cleanup
continues after destination configuration is removed, releasing expired task pins
and aging out final receipts.
Delivery still requires the alias to be present in the current configuration. A
configless installation without a notification database starts no worker and creates
no database. A stopped server cannot send notifications. This does not make task
execution survive server shutdown.


## Root workflow notifications

Use `start_workflow(..., notify="builds", notify_events=["terminal", "attention"])`
to reserve notification intent before run creation, or
`subscribe_workflow(workflow_run_id, notify="builds", events=["terminal", "attention"])`
for an existing root run. Omitting events selects both. Repeating a subscription
updates event selection; an unsubscribed or expired subscription is reactivated.
Child IDs are refused with their root identity; builder runs are unsupported.
These MCP tools require a direct caller before destination configuration is read.
`unsubscribe_workflow(workflow_run_id, notify=None)` stops all aliases, or supply one.
`get_workflow_notifications(workflow_run_id)` reads subscriptions and at most 50
recent deliveries. The read-only CLI equivalent is
`polybridge-ctl workflow-notifications <run_id> --json`; management is MCP-only.
The Monitor does not expose subscription management or workflow notifications.

`workflow.settled` reports completed, failed, or cancelled root runs after settlement.
Node completion does not notify. Recovery followed by another failure creates another
failed episode; reason edits during one failed episode do not. Failed episodes
recorded before recovery remain deliverable even if recovery occurs between worker
passes. `workflow.attention` reports questions, attention checkpoints and system
pauses; caller pauses do not notify. Forwarded nested attention produces one root
point with source fields when available. An answered or replaced attention point
is superseded when claimed. Each subscription sends at most 50 attention deliveries;
terminal deliveries remain enabled. Existing-run subscription starts at the current
point and future points, without replaying earlier history.

The root stores the newest 32 attention points and 64 terminal points, with eviction
watermarks. Points evicted before delivery creation cannot be delivered; retained
points carry `points_missed: true` when selected history was lost. Snapshot bodies
also share a 256 KiB bound: an evicted or failed snapshot preserves its minimal
historical identity and never substitutes a later run's result. Attention granularity
is the durable workflow mutation boundary. Nothing is sent while no server worker
runs; stored points and delivery intent resume when one starts again.

Payload version 1 is bounded to 16 KiB. It includes stable notification and
subscription IDs, event key, root identity, status, sequence, observation time,
bounded question/reason/result fields, definition identity and source fields when
available. Artifact metadata is frozen when delivery intent is inserted: at most ten
note-free references, a separate observation timestamp, a truncation indicator and
a `get_workflow_artifacts` detail reference. Artifact content is never sent.
A missing legacy field is null. Legacy and reconciled state snapshots are labelled.
Tree settlement is checked before sending; an unreadable or unwinding tree waits
up to ten minutes, then carries `tree_settlement_unconfirmed: true`. A historical
failed point whose run has already recovered can be sent immediately with that flag.

Completed and cancelled subscriptions settle permanently. Failed or idle active
subscriptions expire after 30 days without a new point. Pending deliveries retry
independently of subscription settlement, using the same leases, destination
validation, HMAC, six-attempt limit and retention as task notifications. At-least-once
applies to created deliveries within those limits; receivers deduplicate on
`notification_id`. The shared 1000-subscription capacity counts task and workflow
rows. Delivery only changes notification storage, never workflow state or work.
A start that creates a run but fails during launch retains its subscription and
identifies the run; inspect it before retrying. Orphan reservations without a run
are removed after five lease intervals. If post-start activation fails, the returned
run includes a notice and the worker recovers the reserved subscription.

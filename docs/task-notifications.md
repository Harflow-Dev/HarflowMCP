# Task completion notifications

Administrators configure `PB_NOTIFY_DESTINATIONS` in the server environment:

```json
{"builds":{"url":"https://receiver.example/hooks/polybridge","secret_env":"PB_WEBHOOK_SECRET"}}
```

Callers supply only the destination alias (`builds`), never a URL or secret. HTTPS
and globally routable resolved addresses are required by default. The trusted
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
subscriptions expire after 30 days, and final states are retained for 30 days.
Pending subscriptions protect their task records from retention. Unsubscribe can
race with an already transmitted request; it cannot recall a request.

Use `start_task(..., notify="builds")` to reserve durable notification intent before
spawning, or `subscribe(task_id, notify="builds")` for an existing task. These
actions, `unsubscribe(task_id, notify="builds")`, and `get_task_notifications(task_id)`
require a direct caller: delegated agents cannot use server HTTP to bypass their
own network or publishing restrictions. Workflow notifications are unavailable.
A known rejected start releases its unused reservation; an interrupted start may
leave a reservation that expires after 30 days. Inspect the returned task identity
before retrying an uncertain dispatch.
Capability refusals retain their specific explanation when dispatch explicitly marks
the failure as occurring before process creation, no task is registered, and its
untouched reservation was removed. Missing task records alone cannot establish that
no process started: persistence can fail after process creation. If cleanup
or dispatch remains uncertain, the error instead includes the reserved task ID
for inspection before retrying.
Status reads use an existing database read-only, without initializing or changing
storage; an absent database returns an empty list and does not start a worker.

Workers resume when a server opens its task registry if destinations are configured
or a notification database already exists. Cleanup continues after destination
configuration is removed, releasing expired task pins and aging out final receipts.
Delivery still requires the alias to be present in the current configuration. A
configless installation without a notification database starts no worker and creates
no database. A stopped server cannot send notifications. This does not make task
execution survive server shutdown.

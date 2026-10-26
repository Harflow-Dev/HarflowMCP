# Shared scratch folders

Every write-capable task already gets a private, throwaway scratch folder
(`<state root>/scratch/<task_id>`). That folder is retained with the task record and is a new, empty
one for every resume and every continuation, so a screenshot or log written by one run is not where the
next run in the same conversation looks. Agents that are not told a folder exists write to `/tmp`
instead, which the operating system may clear.

Two further folders outlive a task and are shared on purpose:

| Variable | Folder | Shared by |
| --- | --- | --- |
| `HARFLOW_SCRATCH` (alias `HARFLOW_TASK_SCRATCH`) | `<state root>/scratch/<task_id>` | this task only; throwaway |
| `HARFLOW_CHAIN_SCRATCH` | `<state root>/shared-scratch/chain/<chain id>` | a task and every resume and continuation descended from it |
| `HARFLOW_GROUP_SCRATCH` | `<state root>/shared-scratch/group/<slug>-<hash>` | every task started with the same `group` |

`HARFLOW_GROUP_SCRATCH` is unset for a task with no group. All backends receive the variables; inherited
values are always replaced, never passed through. The paths are also in the task's `scratch` status
object, so a caller can find outputs without parsing an agent's report.

## Paths and identity

* The **chain id** is the first task's id. `resume_task` and `continue_task`, and an automatic
  continuation, carry the parent's chain id. A record written before this feature has no chain id and
  is read as its continuation root, then as itself. `chain_root_task_id` itself is unchanged: it still
  drives continuation context and the continuation lock.
* A **group folder** is named `<slug>-<hash>`: the group lowercased, anything outside `[a-z0-9._-]`
  replaced by `-`, runs collapsed, at most 48 characters, then 16 hex digits of the SHA-256 of the exact
  group string. Distinct groups never share a folder, and group text never becomes a path component, so
  `../x` or a very long name cannot escape or collide. The exact group string is recorded in bridge-owned
  metadata (`<state root>/shared-scratch/.meta/<kind>/<key>.json`, never inside the agent-writable
  folder), and reopening a folder verifies it and fails closed on a mismatch.
* The group a task belongs to is the one it was started with; a resume follows the resumed record's
  resolved group (a nested caller's group if there is one, otherwise the parent's), as it already does for
  `group` itself.
* Folders are created `0700`, refuse symlinks in the namespace, and are created only under
  `<state root>/shared-scratch`. Per-task scratch is unchanged.

## What an agent may write

Exposure is a statement about what Harflow granted, never about what the backend might allow. Each folder
has one of three states in `scratch.access`:

* `writable`: Harflow added an explicit write grant. Claude and Codex get one repeated `--add-dir` per
  folder (task, chain, group, in that order, before any extra writable paths) and the folder is in
  `enforcement.writable_roots`.
* `no_additional_grant`: no grant was added. The backend does not confine writes (opencode, vibe,
  Antigravity, and Claude when read-only), or approved commands may still write. The folder exists and the
  variable is set; the claim is only that Harflow added nothing.
* `sandbox_read_only`: the folder is exposed but the OS sandbox leaves it read-only (Codex read-only). Codex
  with caller approvals keeps no scratch grant at all (`HARFLOW_SCRATCH` is then unset) and reports
  `no_additional_grant` for the shared folders: an approved command may run outside the sandbox, so the sandbox
  alone is not a promise that they stay read-only.

A `read_only` task gets the folders and variables (it may read earlier outputs) and no write grant.
Native workflow children get no shared folders. `scratch.notice` states the actual state of each path.

### Nested dispatch

A task started by another task through Harflow is granted write access to a shared folder as a grant only
within what its dispatcher may itself write. Where the dispatcher's writes are not confined (everything but
Codex's sandbox) nothing is restricted. A **confined** dispatcher (Codex) cannot widen into another family's
folder by renaming a group or resuming a sibling's chain: the folder must already be one of the dispatcher's
own recorded writable roots, or (chain only) a brand-new chain, or a chain whose root task was itself
dispatched by it (strict `spawned_by` ancestry, not merely a shared root task). Anything else is exposed
read-only with the real state in `scratch.access`, and a denial persists across hops: a grandchild of a
task holding a folder read-only is denied too.

## Lifecycle

Nothing removes a shared folder on task completion, server restart or recovery, and per-task retention
never touches one.

```bash
harflow-ctl scratch list [--json]
harflow-ctl scratch clean (--group NAME | --chain ID) [--json]
```

* `scratch list` shows each folder's exact group or chain id, path, size (a bounded walk like `du`;
  symlinks are counted, never followed, and `bytes_truncated` says if the bound cut it short), last use,
  last task activity and the number of unsettled users (`active_tasks`: tasks, launches and reserved
  continuations still using the folder, each counted once). `blockers` breaks every reason the folder cannot be
  cleaned down by kind: `active`, `unreadable` (records that cannot be read, which block every folder), `pinned`
  and `artifact`.
* `scratch clean` is for a verified person: it uses the takeover gate and fails closed with `agent_caller`
  or `caller_undecidable`. It refuses with `shared_scratch_in_use` while any task using the folder is not
  conclusively settled (including recovered live tasks and uncertain ones), any launch is pending (including
  a launch waiting for resources) or a reserved continuation names it, any referencing record is pinned (by
  a workflow, or by queued follow-ups), a referencing record cannot be read, or a retained record holds a
  registered artifact under it. Otherwise it renames the folder into `<state root>/shared-scratch/.trash`
  under the folder's lock and deletes it outside the lock, never following symlinks. A folder whose metadata
  is unreadable or invalid refuses a new launch with `shared_scratch_unsafe`, which names this command: once the
  checks above pass, `scratch clean` removes the folder together with the damaged metadata.
* **Age policy.** The retention sweep (a live server, at most once per 24 hours) removes folders unused for
  more than `HARFLOW_SHARED_SCRATCH_DAYS` days (default `14`; `0` or a negative number disables it). A folder
  is eligible only when nothing references it unsettled or pinned, and its age is the newest of its
  `last_used_at` (advanced at allocation and when a task settles), the newest observed `finished_at` among
  retained referencing records, and, for a folder with no metadata, its modification time. A task whose exit
  was never observed (its owner died and recovery later proved it gone) has no known finish time and its start
  time does not count: the sweep first records the moment it found the task settled in the folder's metadata
  (`last_used_at` advances to that moment, once per task), and keeps the folder if that cannot be written.
  Task retention carries a record's last activity into the folder's metadata before the record is deleted (an
  unobserved one counts as now), and keeps the record when that cannot be written, so a shorter task retention
  cannot shorten shared retention. The age policy runs on its own whenever `HARFLOW_SHARED_SCRATCH_DAYS` is
  positive, including when `HARFLOW_RETENTION_DAYS` disables deleting task records.
* **Locks.** Allocation takes the folder's lock (`shared-scratch/.locks/<kind>-<key>.lock`, chain before
  group) before it checks or creates the folder and holds it across the agent's spawn until the task record is
  durable; the descriptor is not inherited by the child. Cleanup takes it for the whole check-and-rename, and so
  does every metadata update (settlement, retention carrying a record's activity), which is why a stale write
  cannot move a folder's last use backwards. The wait is bounded: a launch that cannot get a lock within a few
  seconds fails as not started with `shared_scratch_busy`. Lock order is the session or chain lock, then the
  chain folder's lock, then the group folder's lock, then `<id>.lock`; code that already holds `<id>.lock`
  (the retention sweep) and the settlement hook only try the lock for a fraction of a second, so the order
  cannot deadlock, and give up (keeping the record) rather than wait. A cleaner unlinks the lock file while it
  holds it; a waiter that wins the old file checks it is still the file at the path and retries otherwise.

`get_task_artifacts` reports an artifact whose shared folder was removed as missing (`deleted`).
`register_artifact` accepts a file inside a chain or group folder the task was granted, like its own scratch;
its defaults are unchanged.

## Guidance for agents

Harflow also hands a short static note (about 500 bytes, no paths) to the agent through a system-prompt channel
where one was measured that grants nothing else: `--append-system-prompt` on Claude, `-c developer_instructions`
on Codex (and the app-server's `developerInstructions`), and a read-only instructions file on opencode. The
caller's prompt is never prefixed or rewritten, so a leading `/slash` command stays first. Claude and Codex read
the note once, when the session starts, and keep it; opencode reads it on every run. Vibe and Antigravity have no
such channel, so a task there relies on the environment variables and on the guidance below. `scratch.system_guidance`
says per run whether the note was `delivered`, `reused_from_session` or `unavailable`, and
`capabilities.system_guidance` (in `list_backends`) says how each backend takes it. Nothing depends on the note: it is
never required for a launch. The measurements are in [backend CLI facts](backend-cli-facts.md#system-prompt-seams-for-the-scratch-note-2026-10-27).

For any agent, paste this into `CLAUDE.md` or `AGENTS.md`:

```markdown
## Scratch folders

Do not write intermediate outputs (screenshots, logs, notes, generated files) to `/tmp`: the operating
system may clear it. Harflow-dispatched runs provide scratch folders in environment variables:

- `$HARFLOW_SCRATCH`: private to this task; throwaway.
- `$HARFLOW_CHAIN_SCRATCH`: shared by this task and every resume or continuation of it. Put anything the
  next run in the conversation should find here.
- `$HARFLOW_GROUP_SCRATCH`: shared by every task in the same group (unset when there is no group). Put
  anything sibling tasks should share here.

Prefer these folders over `/tmp`. A folder may be read-only for this run; if a write fails, say so rather
than falling back to `/tmp`. Mention the full path of anything the caller should look at in your report.
```

## Limits

* The note is not available on every backend and is read once per session on Claude and Codex, so a session that
  began before this feature, or on Vibe or Antigravity, relies on the variables and the guidance above; opencode
  is skipped when the caller's own environment sets `OPENCODE_CONFIG_CONTENT`, and workflow builders and native
  children carry no note.
* `scratch.access` reports what Harflow granted. `no_additional_grant` is not a claim that writes are
  impossible, and `writable` is not a claim about the filesystem's own permissions.
* Shared folders are not per-user quota-limited; use `scratch list` and the age policy to watch disk use.
* Detail behind these rules: [architecture](architecture.md#shared-scratch-shared_scratchpy).

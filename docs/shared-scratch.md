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
  `enforcement.writable_roots`. The opt-in read-only group report profile below instead grants
  only the group directory through a native filesystem permission profile.
* `no_additional_grant`: no grant was added. The backend does not confine writes (opencode, vibe,
  Antigravity, and Claude when read-only), or approved commands may still write. The folder exists and the
  variable is set; the claim is only that Harflow added nothing.
* `sandbox_read_only`: the folder is exposed but the OS sandbox leaves it read-only (Codex read-only). Codex
  with caller approvals keeps no scratch grant at all (`HARFLOW_SCRATCH` is then unset) and reports
  `no_additional_grant` for the shared folders: an approved command may run outside the sandbox, so the sandbox
  alone is not a promise that they stay read-only.

An ordinary `read_only` task gets the folders and variables (it may read earlier outputs) and no write grant.
The explicit `group_report_output` option below enables a separately qualified group-only exception.
Native workflow children get no shared folders. `scratch.notice` states the actual state of each path.

### Read-only review reports

Set `group_report_output: true` together with `freedom: "read_only"` and a nonempty `group`
to request report output while preserving read-only repository access:

```bash
harflow-ctl permission-preview --repo /path/to/repo --backend codex --freedom read_only \
  --group review-batch --group-report-output --json
harflow-ctl run --repo /path/to/repo --backend codex --freedom read_only \
  --group review-batch --group-report-output "Review the change and save your findings"
```

MCP `start_task`, `resume_task`, `continue_task` and `preview_task_permissions` accept the same
`group_report_output` boolean. Preview also accepts `group`; it computes the concrete canonical
folder without allocating it and checks the backend's prospective configuration. Preview is an
observation, not a reservation; launch rechecks identities and configuration after allocation.
Cached-only qualification discovery reports the `group_report_output` feature; an opted-in
cached-only preview requires an explicit group rather than discovering caller authority.

The initial supported combination is **Codex CLI 0.165.0 on macOS, headless read-only, network
disabled**. The native profile extends `:read-only` and adds exactly one write root: the task's own
canonical group directory. It does not add the repository, task scratch, chain scratch, `/tmp`,
other groups, or protected configuration to the writable set. `enforcement.group_report_output`
is true, `enforcement.writable_roots` names only this group folder, and `scratch.access.group` is
`writable`. The group path is available as `$HARFLOW_GROUP_SCRATCH`; task scratch remains unset.
A missing group is an error; there is no implicit standalone group.

Qualification is version/platform specific and currently uses measured feature evidence. Launch
and preview refuse inherited legacy sandbox or permission-profile selectors, project or managed
execution configuration, hooks, MCP or plugins that the narrow native profile cannot certify.
The CLI runtime and configuration namespaces must lie outside the writable report root.
Changing user configuration to satisfy this feature is never automatic. Other versions, platforms,
Claude plan mode, opencode, Vibe and Antigravity explicitly refuse this option. Omit it to retain
the existing backend behavior and return findings for engine-generated output. In measured Claude
2.1.312 probes, scoped Write/Edit allow rules and additional directories did not override plan
mode's refusal to write ordinary report files.

Caller approvals, native children, workflow builders, extra writable path grants and other freedom
levels cannot be combined with this option. Interactive takeover is refused, and no ambient
interactive resume command is offered; use headless resume or continue instead. `None`/omission on
resume or continuation inherits the saved opt-in; explicit false (`--no-group-report-output`)
disables it. An opted-in resume/continuation cannot change the saved group. Ordinary tasks remain
off by default, including grouped read-only tasks.

Saved workflow agent candidates may opt in only at `read_only`. Opted-in nodes share the explicit
`workflow:<root-workflow-run-id>` group across the root workflow and nested child runs. Root-run
identity comes from the workflow's existing parent link; no unrelated run or caller group is
implicitly granted. Native execution requests cannot represent this exception and are refused
or select headless execution according to the existing execution policy.

Use `report-$HARFLOW_TASK_ID.md` so concurrent reviewers write distinct files. Create a temporary
file inside the same writable group and atomically replace the final report when updating it.
Peers in the same authorized group can read earlier reports; a full group-directory grant also
allows them to update each other's files, so filename ownership and report-content arbitration
are conventions, not an enforced boundary. Do not place executable configuration in report output.
Native scope checks deny traversal and symlink writes that resolve outside the authorized group;
prelaunch identity checks do not atomically pin paths against independently authorized concurrent
external writers.

### Nested dispatch

A task started by another task through Harflow is granted write access to a shared folder as a grant only
within what its dispatcher may itself write. Where the dispatcher's writes are not confined (everything but
Codex's sandbox) nothing is restricted. A **confined** dispatcher (Codex) cannot widen into another family's
folder by renaming a group or resuming a sibling's chain: the folder must already be one of the dispatcher's
own recorded writable roots, or (chain only) a brand-new chain, or a chain whose root task was itself
dispatched by it (strict `spawned_by` ancestry, not merely a shared root task). Anything else is exposed
read-only with the real state in `scratch.access`, and a denial persists across hops: a grandchild of a
task holding a folder read-only is denied too. A `continue_task` takes the source's lineage, but the grant
is bounded by the actual caller as well: a confined caller continuing another family's task gets that
folder read-only. An explicit `group_report_output` request instead fails if its group
write grant is outside either the source dispatcher's or the current caller's authority; it is never
silently downgraded.

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


## Task-named plans and review reports

After an observed process exit and complete output drain, Harflow materializes the final
summary verbatim as `<state root>/outputs/<task_id>/report-<task_id>.md`. A final workflow
JSON result containing `result.technical_plan` also produces `plan-<task_id>.md` alongside it.
Both are registered as report artifacts with `engine_output` provenance: use task artifact
reads (and `artifact_summary` in task results) to discover them. These files are engine
outputs, **not agent-writable locations**; no repository or home write permission is added.
Empty output produces no file; output above 16 MiB is not materialized. An unavailable output
file is disclosed in task notices and does not change the task outcome; the summary remains
available. Publication refuses existing files and symlinks rather than overwriting them.
Outputs are retained and removed with their task records. Resumes and continuations have new
task IDs and output paths, so identical titles and concurrent reviews cannot collide.

During a write-capable task, save and update working plans in writable task scratch as
`plan-<task_id>.md`, checking `scratch.access.task` first. In Claude plan mode, use Claude's
built-in plan location under its existing permission policy, then return the final plan in
`result.technical_plan` for a task-named published copy. Harflow does not control Claude's
internal random plan filename or redirect it by changing user settings. A human-configured
project `plansDirectory` of `.claude/plans` is supported by the measured CLI; Harflow's
approval protection excludes only ordinary descendants of this project directory. All other
`.claude` configuration, unknown directories, `.mcp.json`, and symlink escapes remain protected.
This exception does not preapprove harness actions or make arbitrary paths writable in plan mode.
Reviewers should return their complete report in their final response rather than creating
an unrelated repository file or changing settings to enable report writes.

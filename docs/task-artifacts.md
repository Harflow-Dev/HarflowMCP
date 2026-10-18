# Task artifact references

Dispatched standalone tasks and verified workflow attempts can call `register_artifact(path, kind, note)` to retain
metadata references to screenshots, logs, reports and other regular files. Registration
does not read, copy, upload, publish or preserve file contents. It is not proof of
content validity, file authorship, successful execution, or permission to read, copy or
publish a file. A failed task can have registered artifacts.

## Register from a dispatched task

Use the MCP tool or run the CLI from inside the dispatched task's process tree:

```python
register_artifact(path="reports/check.txt", kind="report", note="Observed checks.\nNeeds review.")
```

```bash
polybridge-ctl register-artifact reports/check.txt --kind report \
  --note 'Observed checks; needs review.' --json
```

Kinds are `screenshot`, `log`, `report` and `other`. They are the registrar's
declarations; Polybridge does not inspect the contents to certify the kind. Notes are
stored verbatim, including whitespace. Oversized metadata is refused, not silently
shortened in storage.

Registration derives the owner from verified process lineage. There is no task ID
parameter that lets a caller choose another owner. Human sessions, spoofed or ambiguous
task identities, and tasks that have settled cannot register. The registrar identity
describes who submitted the reference, not who created the contents.

Caller discovery uses the task catalog and considers at most 100 identities related
by process ancestry or session. Overflow or incomplete/unreadable authority fails
closed. MCP and CLI requests share a raw metadata budget of 8 MiB and 100 JSON
attempts across task and workflow catalog preparation, every owner and ancestor
reload (including registration's fresh checks under the lock), and manifest reads.
Task and workflow records have a 4 MiB per-file limit; manifests have a 1 MiB limit.
Malformed JSON consumes its bytes and decode attempt. Budget exhaustion fails closed;
registration does not replace the manifest. Retry an incomplete preparation request
to advance the catalog. Nested readers share the budget; a new request starts fresh.
Ready requests do not deserialize unrelated historical records. Caller discovery's
derived SQLite projections are separately limited to 100 rows of at most 8 KiB each.
Target availability, protected configuration paths and saved grant identity checks
inspect filesystem metadata without reading target or configuration contents. Prompt
sidecars and execution logs are not opened by artifact authorization. These limits
cover the artifact path; they do not change the separate full status recovery read.
These content bounds do not guarantee constant time for filesystem enumeration,
stat calls or SQLite operations.

The selected owner's identity and ancestry are checked before authorization. Workflow
ownership uses a bounded receipt checked against the run, activation and task. Node,
orchestrator and native-control attempts may register while reserved, running or uncertain.
The saved `workflow` binding records root/run/node/execution/task identity, role, visit/retry
identity when present, and `workflow_receipt` provenance. Resumed ancestors must belong to
the same root tree. Builders, inherited descendants without their own receipt, mismatched
receipts and unverifiable catalog associations are refused. A registration may race with
settlement after the unlocked run check; its immutable binding still identifies the attempt.
Native children have no registration channel of their own: references registered through
their hosting transport are labelled `native_transport`, with hosted attempt IDs (including
batch ambiguity), and never claim child authorship. Workflow extra writable paths remain
unsupported; workflow scope is the repository and the owning task's scratch directory.

The MCP channel requires Polybridge registered in that harness and its MCP approval.
The CLI channel requires a shell able to write `~/.polybridge/tasks` and run `ps`.
OS sandboxes, including Codex read-only/workspace-write and native profiles with empty
writable roots, block that CLI path. These are channel requirements, not a claim that every
harness is certified to support registration. No permission is added by registration.

## Paths and observations

Relative paths resolve against the owning task's recorded repository directory.
Registration requires an existing accessible regular file in that repository, the
task's own allocated scratch directory, or an explicitly granted path whose saved
identity still validates. Merely being able to access a path from the server does not
authorize registration. Other tasks' scratch, bridge control state and protected
execution configuration are not artifact scopes. Symlink destinations must satisfy
the same checks. Directories, sockets, devices, FIFOs and initially missing paths are
refused. No filesystem or network permission is added by registration.

The receipt records a stable artifact ID, path, kind, note, registration time, registrar
provenance and observed device/inode/size/mtime metadata. Repeating an identical
registration with the same observed identity returns the existing reference; changed
metadata produces another immutable registration. References from different tasks are
never merged simply because their paths match.

Reads report a separate timestamped availability observation:

| State | Meaning |
| --- | --- |
| `available` | The regular path was accessible and its observed metadata matched |
| `changed` | The path or observed file metadata changed since registration |
| `deleted` | A previously registered file is no longer present |
| `unavailable` | Access, scope, metadata or identity could not be established safely |

An `available` observation does not verify bytes. Stat metadata cannot detect every
content change, and a file can change immediately after an observation. A reference
does not reserve the path. Escaping symlinks are not followed to inspect unrelated
targets during availability checks.

## Read metadata

```python
get_task_artifacts(task_id="<task_id>", limit=20)
# Continue with the returned next_cursor until has_more is false.
```

```bash
polybridge-ctl task-artifacts <task_id> --limit 20 --json
polybridge-ctl task-artifacts <task_id> --limit 20 --cursor '<next_cursor>' --json
polybridge-ctl status <task_id> --brief --json
```

A verified human caller can inspect a known task, including workflow-bound tasks. A dispatched
task can inspect its own references. Metadata reads do not grant file-content access.
Saved extra-path grants are structurally validated together, then each covering grant
is revalidated independently. One valid
covering grant suffices; a changed unrelated grant does not revoke it.
Task status returns a bounded `artifact_summary` and a detail reference, rather than
injecting complete notes or file contents into task results. Compact status retains
its 8 KiB response budget. Task listings do not stat every referenced file.

At most 100 references are retained per task. Path and note fields are each limited to
4096 UTF-8 bytes, and a task manifest is limited to 1 MiB. Pages contain at most 50
entries and fit a 64 KiB serialized response budget. A page may contain fewer entries
than requested. Cursors are bound to the task and manifest revision; restart paging
when concurrent registration makes a cursor stale. Notes remain complete in detail
pages. No file-content payload is included, regardless of referenced file size.

Supported standalone tasks without a manifest have an empty registration set.
Unsupported or unverifiable ownership (including workflow builders and unbound inherited
descendants) reports an unavailable summary without a detail reference;
its manifest is not opened for that summary. An unreadable or
malformed manifest is reported as unavailable, not as an empty successful result, and
cannot be overwritten by a new registration.

## Monitor, privacy and retention

The task Summary shows artifact metadata separately from edited files and execution
status. Conversation members retain their individual registrar/task identities.
Additional references are loaded in bounded pages. Older CLIs without artifact reads
show the capability as unavailable. The view does not automatically preview contents
or execute registered files.

Paths and notes can disclose sensitive information even without file contents. Submit
only metadata appropriate for the task's readers; Polybridge does not certify that
notes or file contents are nonsensitive. Artifact metadata is not added to completion
webhooks by this change.

Registration metadata follows its owning task record's retention. Existing task scratch
cleanup can delete a referenced scratch file. Files in repositories or external granted
locations are never deleted by artifact-reference cleanup. Registration is not archival
storage. Concurrent registration and retention share the task lifecycle lock, and a
receipt is returned only after its manifest is durably written. Retention preserves the
exact resume-parent, dispatch-parent and continuation-source records needed to verify
each manifest owner's standalone ancestry. In-flight snapshots conservatively preserve the same ancestors
so a registration finishing during a sweep cannot lose that evidence. The owner
expires normally; those ancestors may remain until a subsequent sweep. If owner
deletion fails, its ancestors remain for the retry. Ancestor preservation does not
extend the lifetime of the owner's descendants or session peers.

The CLI JSON contract is version 13; setup and event-log versions are unchanged.

## Workflow tree reads and selected delivery

```python
get_workflow_artifacts(workflow_run_id="<run_id>", execution_id="<execution_id>", limit=20)
```

```bash
polybridge-ctl workflow-artifacts <run_id> --execution-id <execution_id> --limit 20 --json
```

Humans can read any run. Delegation orchestrators can read their own run and linked
descendants. Nodes and native controls can read only their own task references; standalone
callers, builders and legacy non-delegation managed callers cannot read run-level pages.
Task artifact summaries are available for bound tasks and retain the existing summary shape.

Workflow pages use a ready catalog-only tree projection, then lazily read records under the
shared metadata budget. Cold, invalidated or partially prepared catalogs report
`catalog_preparing`; each retry advances a separate bounded catalog-preparation slice
under the same request budget, then a later request queries the ready catalog. Normal run
listings also prepare the catalog. Missing linked children report
`catalog_stale`. Trees above 64 runs report `tree_bound_exceeded`. Pages contain at most 50
items, at most 50 exceptional owner rows, and a serialized envelope of at most 63000 bytes.
Unreadable records/manifests report `run_unavailable`/`unavailable`; expired owners report
`record_removed`. Cursors are at most 512 characters and bind run, activation/task position,
owner identity and last artifact ID. Ordering is run creation time/ID, activation order, then
task order, with task/artifact deduplication. References appended behind a cursor are visible
on a fresh pass. Budget-limited pages can contain no items while still advancing past owners.

The Monitor's selected execution has an Artifacts section with explicit paged loading,
Deleted, Unavailable and Owner record removed rows, and native transport attribution.

Orchestrator context lists whole references of settled executions of the current run, capped
at 32 items and 8 KiB. It selects at most eight unique `{execution_id, artifact_id}` pairs in
`next[].artifact_refs`, including branch assignments, retries and workflow invocations.
Deleted, unavailable, unsettled, unknown and out-of-scope selections are decision errors;
changed references are allowed and labelled. Frozen references are stored on the activation
and token and rendered before input results, with notes verbatim and a 16 KiB section cap.
Only metadata is delivered; no file content is injected. Notes are data, not instructions.

A workflow invocation passes selected references separately from automatic input results.
Inside the child they are listed as inherited and require selection again before reaching a
worker or grandchild. Unselected parent references are not exposed. Recovery preserves stored
bindings and selections; retries retain distinct execution identities. References follow the
owner task's retention, and workflow tasks remain pinned while their tree is active.

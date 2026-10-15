# Task artifact references

Standalone dispatched tasks can call `register_artifact(path, kind, note)` to retain
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
closed. Cold MCP and CLI requests prepare at most 100 metadata records and 8 MiB of
metadata per request; retry a preparation error to advance the catalog. Registration's
two identity checks share that preparation budget. Ready requests do not deserialize
unrelated historical records. The selected owner is reloaded with a 4 MiB metadata
limit, and its identity and standalone ancestry are checked before authorization.
Standalone exclusion also uses the bounded workflow ownership catalog, checking each
ancestor and refusing any ownership receipt, including malformed or unavailable
receipts. Incomplete workflow preparation requires a retry; summaries report
`unavailable` until ownership can be established. Task and workflow preparation share
the request's metadata budget. Artifact paths never fall back to scanning full
workflow history.

Workflow orchestrators, workers, builders, native children and inherited workflow
descendants, including continuation successors, are not supported by this standalone
contract. Workflow ownership and output selection belong to #82; workflow extra
writable paths belong to #81.

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

A verified human caller can inspect a known standalone task. A dispatched standalone
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
Unsupported or unverifiable ownership (including workflow builders, managed workers,
and their descendants) reports an unavailable summary without a detail reference;
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

The CLI JSON contract is version 11; setup and event-log versions are unchanged.

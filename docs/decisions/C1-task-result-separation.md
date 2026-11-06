# C1 candidate: task result separated from background obligations

Candidate `task-result-v1`, reserved capability `task-result-separation.v1`. Models MCP #110, where a
leftover background command abandoned at the idle bound makes a successful task show as failed. **It does not
fix #110**: it gives the separation a name and pins it, and the shapes that most often cause #110 still fail
or block under it unless a caller declares the requirement (see "Where #110 still bites").
Specified, unimplemented and **not advertised**: Python 0.0.11 does not implement it and its
`version-info` capability list, `contracts/v1` and every consumer gate are unchanged. Assets:
`contracts/candidates/task-result-v1/` (manifest, digest and specimens), generated and checked by
`scripts/generate-c1-candidate-fixtures.py`.

This is the **public projection** of the internal model in
[owner-runtime-v1](owner-runtime-v1.md) and its [schema](owner-runtime-v1-schema.md), whose observation
table is authoritative for every decision below (owner decision 2, approved). The candidate adds no
decision of its own: it names the fields a `harflow.ctl` `status` reply would carry and pins that they
equal the model's output.

## Projection

An additive optional part of the `task` object of `status`, `status-batch` and task-list rows (a consumer
that does not know the capability ignores it):

| Field | Values | Meaning |
| --- | --- | --- |
| `agent_result` | `unknown`, `succeeded`, `failed` | the display result: sticky failure wins; a result not bound to the current attempt, or hidden by an unresolved later turn, is `unknown` |
| `background.requirement` | `required`, `incidental`, `unknown` | declared by the qualified caller or the saved workflow contract; missing or unclassified is `unknown`, never `incidental` |
| `background.obligations` | `pending`, `satisfied`, `failed`, `unknown` | one aggregate lifecycle |
| `background.inventory` | `complete`, `partial`, `unknown` | partial or unknown inventory is never an empty successful inventory |
| `settlement.workflow` | `blocked`, `succeeded`, `failed`, `cancelled` | whether the workflow may settle on this task |
| `settlement.reason` | `unsettled`, `obligations`, `execution_unknown`, `cancelled`, `execution_failed`, `result_failed`, `required_obligation_failed`, `succeeded`, `result_unknown` | first applicable rule of the model |
| `settlement.release` | `blocked`, `evidence_complete` | an observation that resource-release evidence is complete; never a release grant |

The decision rules are exactly the observation rules of the owner-runtime schema (workflow order:
unsettled, obligations, execution unknown, cancelled, execution failed, result failed, required
obligation failed, succeeded, otherwise result unknown). The consequences for #110:

- Only when the caller has declared the obligation **incidental**: an incidental obligation that fails (or is pending) leaves `agent_result: succeeded` and
  `settlement.workflow: succeeded`. A pending incidental obligation keeps `settlement.release: blocked`
  (its resource claims stay held); a failed one with complete evidence does not.
- A **required** failed obligation settles as `failed` with `required_obligation_failed`.
- Unknown requirement or unknown obligations block; they are never treated as incidental.
- An execution failure, cancellation, unresolved later turn, stale result or capture gap is never hidden by
  an incidental obligation.
- `agent_result`, `settlement` and release are three separate values and are never collapsed.

Existing `outcome` (observed Git, artifact and command facts) keeps its meaning and is not reused.

## Specimens

`contracts/candidates/task-result-v1/*.json` hold `{format: harflow.c1.candidate, v: 1, candidate, kind:
projection, input, document}`: `input` is the owner-runtime observation data, `document` the projection a
reply would carry, and the manifest records the expected classification. The checker recomputes the
projection from `input` with the model and fails when the document differs (`projection_mismatch`),
including a document that lets an incidental failure rewrite a succeeded result. Unknown enum values are
`unsupported`, a missing or mistyped field is `invalid`, a future `v` or `kind` is `unsupported`.
Documents are at most 24576 bytes, depth 16, strings 4096 bytes, arrays 64.

## Where #110 still bites

- **A held pipe.** A background command that keeps stdout or stderr open leaves the stream not at EOF, which the
  model treats as `unsettled` whatever the requirement (`held-pipe-blocks-settlement.json`). The task stays
  blocked; the candidate does not abandon it.
- **An undeclared caller.** With no declaration the requirement is `unknown`, which the model treats as
  required: a failed leftover obligation fails the task (`undeclared-requirement-failed-obligation-fails.json`).
  Existing callers declare nothing, so for them the #110 symptom is unchanged.
- **Abandonment at the idle bound** is not mapped to any input. Whether it becomes `obligations: failed`, an
  `execution: failed` or an unobserved exit is undefined, and the answer decides which rule above applies.

## Not decided here (open owner questions)

- The **default requirement** for a caller that declares nothing. The design text says missing or unknown
  remains required for settlement; keeping that leaves #110 for every existing caller.
- The legacy `status` value for a settled task is not defined. The design text says the existing terminal
  status is not renamed into a result field and that an incidental failure cannot rewrite a valid result into
  failure; it does not say what `status` shows. A mapping (`succeeded` to `completed`, `failed` to `failed`,
  `cancelled` to `cancelled`, `blocked` to unchanged non-terminal with retained attention) is the obvious
  reading but is **not** specified.
- **How abandonment at the idle bound maps to the observation inputs** (see above).
- Per-obligation items are not part of the projection: the design text ("Immutable policy and binding") lets
  the initial policy classify the operation's background work as one aggregate and says finer per-obligation
  authority is not invented.
- How a caller declares the requirement (ctl option, MCP parameter, saved-workflow field) and how each
  backend reports an agent result witness are not defined.
- Python behaviour is unchanged until the owner assigns an exception.

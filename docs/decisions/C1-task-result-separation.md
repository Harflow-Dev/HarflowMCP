# C1 candidate: task result separated from background obligations

Candidate `task-result-v1`, reserved capability `task-result-separation.v1`. Addresses MCP #110: a
leftover background command abandoned at the idle bound makes a successful task show as failed.
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

- An **incidental** obligation that fails (or is pending) leaves `agent_result: succeeded` and
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

## Not decided here (see the morning questions file)

- The legacy `status` value for a settled task is not defined. The owner decision says an incidental failure
  cannot rewrite a valid result into failure and that terminal status is not renamed into a result field.
  A mapping (`succeeded` to `completed`, `failed` to `failed`, `cancelled` to `cancelled`, `blocked` to
  unchanged non-terminal with retained attention) is the obvious reading but is **not** specified.
- Per-obligation items are not part of the projection (decision 2 permits only one aggregate).
- How a caller declares the requirement (ctl option, MCP parameter, saved-workflow field) and how each
  backend reports an agent result witness are not defined.
- Python behaviour is unchanged until the owner assigns an exception.

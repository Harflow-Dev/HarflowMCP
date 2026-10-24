# Workflow context delivery benchmarks

These deterministic fixtures measure injected UTF-8 prompt bytes. They use fake
harnesses and make no model calls. They do not estimate tokens, harness history,
model cache effectiveness, model quality, or dollar savings. Reported usage and
cost remain `null` when the fake harness supplies neither.

## Complete workflow comparison

Run the real workflow supervisor against fake registries:

```bash
uv run pytest tests/test_workflow_context_integration.py::test_whole_workflow_context_benchmark -q -s
```

The test emits one `WORKFLOW_CONTEXT_BENCHMARK` JSON report. It sums the exact
persisted reservation `context_delivery.total_bytes` over **every dispatched
task**, including workers and Fresh/resumed orchestrator turns. Groups include
role, session mode, and classification, with explicit repair/inspection counts.

Both runs use the same graph, objective, focused assignments, and worker results:
Start → work → verify → End. Assignments contain repeated compatibility
constraints to exercise historical assignment compaction. Both run with the
guided policy and Headless workers. Optimized decisions echo the issued context
acknowledgement; legacy decisions use the legacy envelope.

The fixture checks matching workflow outcomes and dispatch counts. Temporary paths
appear in scratch instructions, so absolute byte totals vary with the environment.
The test asserts behavioral outcomes and total-byte reduction rather than fixed
machine-specific byte counts.

## Isolated prompt scenarios

Run:

```bash
uv run python scripts/benchmark-workflow-context.py
# Compare a saved report from the previous implementation:
uv run python scripts/benchmark-workflow-context.py --baseline-report baseline.json
```

The optional baseline comparison reports the prior prompt version and signed byte change
for matching fixtures. Scenarios absent from the prior report are labelled as new evidence;
changed legacy payload sizes make a row incomparable. Negative bytes saved disclose regressions.

This separate report exercises individual rendered contexts rather than complete
workflow execution. It includes section accounting and explicit `model_calls: 0`
and `token_counts: null`. Parallel evidence manifests are synthetic authorized
retrieval fixtures; their IDs cannot be used against a live workflow.

Large-history savings come mainly from removing historical assignment bodies.
Parallel evidence savings include replacement with complete retrieval references.
Repair keeps bootstrap; an acknowledged compatible resume omits it. Internal prompt version 2
also reports per-section full/reference/append modes. The acknowledged-sections scenario
retains a large unchanged technical plan and instruction body by digest reference and appends
one checklist item against a verified list prefix. Cache counts and cost remain null.

## Recovery and evidence coverage

The integration suite also verifies exact/missing/stale acknowledgements,
protocol repair bootstrap, positively refused candidate fallback, explicit legacy
upgrade, retained-session supervisor restart, authorized oversized worker input,
linked-child evidence reconstruction, and exhausted orchestrator inspection
budgets. These assertions cover recovery and evidence access independently of
the complete workflow comparison.

```bash
uv run pytest tests/test_workflow_context_integration.py tests/test_workflow_assigned_inputs.py -q
```

## Integrated implementation prompt comparison

A no-model renderer comparison of the integrated context, completion-offer and structural
implementation against commit `4654bba` uses the same
`acknowledged-sections` scenario and exact predecessor/next context in both
implementations: a repeated technical plan, 20 checklist items, a repeated
verbatim node instruction body, then one appended checklist item at `d2`.
Both implementations receive and acknowledge the predecessor before rendering
the next context. This measures section reconstruction only; it does not claim
savings from dispatch-time graph projection or different routing behavior.

| Injected UTF-8 bytes | Version 1 baseline | Integrated version 2 |
| --- | ---: | ---: |
| First prompt | 43,067 | 45,254 |
| Acknowledged next prompt | 42,809 | 5,738 |
| Both prompts | 85,876 | 50,992 |

The next prompt saves 37,071 bytes; the pair saves 34,884 bytes. The unchanged
plan and instructions use references, and the checklist uses an exact-prefix
append. These are prompt byte measurements, with model calls zero and token,
cache and cost counts unavailable.

The seven earlier isolated fixtures lack these large reusable sections.
At the context-delivery stage (`2687a49`), comparison with their saved version 1 report
showed 1,740 additional bytes in each
bootstrap scenario and 1,142 bytes to the resumed scenario for explicit section
metadata and reconstruction/handoff guidance. This fixed overhead is a
regression for those small contexts; legacy-versus-optimized savings must not
be presented as savings introduced by version 2.

The complete fake workflow comparison also preserves five dispatched tasks,
three orchestrator decisions, two worker executions, and a completed outcome.
Its optimized delivery increases from 65,222 bytes at `4654bba` to 74,736 bytes
with the integrated changes (9,514 bytes more). The current legacy run injects
135,683 bytes; its guidance also changed, so that comparison does not isolate
the effect of section delivery.
This short graph's small instruction bodies and checkpoint sections do not
amortize the added protocol guidance and metadata. Absolute totals may vary
with temporary scratch paths. No routing/decision-count reduction is attributed
to this context-only change.

## Structural runtime comparison

Run the same fake parallel graph through the real guided supervisor:

```bash
uv run pytest tests/test_workflow_traversal.py::test_guided_split_has_one_briefing_and_structural_end_skips_routing -q -s
```

The graph is Start → Parallel start → left/right agents → Parallel end → End,
with the same distinct branch assignments and clean succeeded worker results.
The pre-change runtime requests harness decisions at Start, left, right and End.
The deterministic runtime requests them at Start and End. Both complete with
six transitions and two worker executions; the serial Start → worker → End
case already skipped its final worker routing and is not a new reduction.

| Observation | Pre-change runtime | Deterministic runtime |
| --- | ---: | ---: |
| Harness decisions | 4 | 2 |
| Worker executions | 2 | 2 |
| Transitions | 6 | 6 |

The runtime emits three runner-owned decisions for the two clean branch
arrivals and joined exit. These records are separate from model/harness turns.
The fixture makes zero model calls; token, cache and dollar measurements are
unavailable. Conditions, optional/failure evidence, explicit routing opt-outs
and executable successors require separate safety tests and are not counted
as automatically traversable paths in this comparison.

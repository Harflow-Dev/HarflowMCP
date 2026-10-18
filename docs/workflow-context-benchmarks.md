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
```

This separate report exercises individual rendered contexts rather than complete
workflow execution. It includes section accounting and explicit `model_calls: 0`
and `token_counts: null`. Parallel evidence manifests are synthetic authorized
retrieval fixtures; their IDs cannot be used against a live workflow.

Large-history savings come mainly from removing historical assignment bodies.
Parallel evidence savings include replacement with complete retrieval references.
Repair keeps bootstrap; an acknowledged compatible resume omits it.

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

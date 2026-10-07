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

Observed baseline on 2026-10-07:

| Measurement | Legacy | Optimized v1 |
| --- | ---: | ---: |
| Final status | completed | completed |
| Total dispatched turns | 5 | 5 |
| Worker executions | 2 | 2 |
| Repair turns | 0 | 0 |
| Inspection turns | 0 | 0 |
| Fresh orchestrator bytes, 1 turn | 7,059 | 5,813 |
| Resumed orchestrator bytes, 2 turns | 76,083 | 12,712 |
| Fresh worker bytes, 2 turns | 42,051 | 41,998 |
| Complete workflow injected bytes | 125,193 | 60,523 |
| Reported tokens / cost | unknown | unknown |

The observed reduction is **64,670 bytes (51.7%)** across the complete workflow.
This fixture establishes matching fake workflow outcomes and dispatch counts;
it does not establish live-agent quality. Temporary paths appear in scratch
instructions, so absolute byte totals can vary with the test environment. The
test asserts the behavioral outcomes and total-byte reduction, rather than
hard-coding machine-specific byte counts.

## Isolated prompt scenarios

Run:

```bash
uv run python scripts/benchmark-workflow-context.py
```

This separate report exercises individual rendered contexts rather than complete
workflow execution. It includes section accounting and explicit `model_calls: 0`
and `token_counts: null`. Parallel evidence manifests are synthetic authorized
retrieval fixtures; their IDs cannot be used against a live workflow.

| Fixture | Legacy bytes | Optimized bytes |
| --- | ---: | ---: |
| Small | 4,328 | 2,680 |
| Large history | 225,716 | 3,839 |
| Parallel evidence | 673,884 | 4,770 |
| Child scope | 225,859 | 3,973 |
| Clarification | 225,955 | 4,066 |
| Protocol repair | 225,716 | 3,839 |
| Acknowledged resume | 225,716 | 3,508 |

Large-history savings come mainly from removing historical assignment bodies.
Parallel evidence savings include replacement with complete retrieval references.
Repair keeps bootstrap; an acknowledged compatible resume omits it.

## Recovery and evidence coverage

The integration suite also verifies exact/missing/stale acknowledgements,
protocol repair bootstrap, positively refused candidate fallback, explicit legacy
upgrade, retained-session supervisor restart, authorized oversized worker input,
linked-child evidence reconstruction, and exhausted orchestrator inspection
budgets. These assertions cover recovery and evidence access independently of
the five-turn baseline above.

```bash
uv run pytest tests/test_workflow_context_integration.py tests/test_workflow_assigned_inputs.py -q
```

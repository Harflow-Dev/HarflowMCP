# Backend model listing producer receipt

This receipt covers the additive Python canonical producer for `backend-models.v1`. It does not transfer
contract ownership, qualify a published release, or certify a native consumer or a Rust implementation.

## Scope and integration order

The producer adds `harflow-ctl backend-models --backend NAME [--json]`, the capability, a `Backend.list_models`
seam (default `unsupported`), six measured sources, a bounded two-pipe process runner
(`bounded_process.run`), fixtures, a generator and an independent `validate_backend_models` oracle. MCP
tools, `backends`, ordinary status shapes and launch admission are unchanged; `task_changes_capture` keeps
its own runner. Consumers must negotiate the capability from the selected engine, vendor the entire
byte-identical final bundle, and treat an older engine's usage error for the command as unsupported.
Fixtures keep `product_version` 0.0.13; release preparation re-stamps them. The bundle has 351 fixtures
(0.0.13's 334, sixteen `ctl-backend-models-*.json` specimens and `invalid-s05-missing-backend-models.json`).
Its manifest SHA256 is `7f4ae95ba97e7391dc9cbe4d24b10db64730848143c6690886056f9f9b8d46ba`. `ctl-launch-unknown-operation.json` changes because argparse lists every
subcommand in its usage message; HarflowMac must re-vendor it with the rest of the bundle.

Integration order: this producer and its fixtures, then HarflowMac fixture vendoring, adapter and version gate,
then Rust adoption of the same ctl contract.

## Authority and bounds

The command is a direct-caller read. Environment markers refuse first with no I/O; then lineage runs under a
shared `initialize=False` state session with a 2 second lock timeout. Only a missing state root proceeds;
every other store problem is `caller_undecidable`. Nothing is created or written. One 10 second budget starts
at command start, covers the gate and is shared by every subprocess (8 seconds each at most). Sources are
read-only: documented aliases (no subprocess), the Codex model cache and the Vibe config (bounded regular-file
reads of at most 8 MiB, no FIFO or non-regular file), and the CLI output of opencode, agy and cursor-agent
(1 MiB stdout, 32 MiB for opencode, 64 KiB stderr, process group killed on expiry). Cursor's CLI identity is
verified inside the same budget before it is listed. Rows, labels, ids and the printed line are bounded as
specified in `docs/contracts.md`.

## Verification boundary

Tests use recorded harness outputs (measured 2026-11-13), fake binaries on a temporary PATH, temporary
homes and state roots, and an injected runner and clock. No real harness, model call or real state root was
used to produce the fixtures or tests. The generators were run in dependency order and every generator's
`--check` passes. Pinned manifest count and digest moved in `tests/test_approval_seams.py`,
`conformance/oracles.py`, `conformance/tests/test_oracles.py`, `conformance/tests/test_runner.py` and
`conformance/README.md`. The Rust `ctl_capability_order.rs` is not touched, because it changes only when Rust
emits the capability. This receipt makes no installed-engine, native UI, Rust or
release qualification claim.

## Later verification (2026-11-14)

Full pytest at `78c6982`: 15006 passed, 559 skipped, 4 failed. Three failures were `ENOSPC` on the host temp
volume and the fourth a start-up timing margin in a new test (widened in `300c021`); all four pass when re-run.
`5ce07d1` (bounded_process only) was verified with its targeted suites. Live check from the development
environment, read-only against the developer's own state root: `--backend opencode` returned `available`,
`cli_output`, 199 models (158 with effort levels), 24,670 bytes in 1.8 s; `--backend codex` returned
`available`, `harness_cache`, 8 models, 1,318 bytes in 0.9 s; neither dropped nor truncated rows.

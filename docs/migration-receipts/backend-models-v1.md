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
Fixtures keep `product_version` 0.0.13; release preparation re-stamps them. The bundle has 350 fixtures
(0.0.13's 334, fifteen `ctl-backend-models-*.json` specimens and `invalid-s05-missing-backend-models.json`).
Its manifest SHA256 is `092299de1f3ed1ac91c55d168ce3f77c47e3d4f6859b9ebefb416393e149aea1`. `ctl-launch-unknown-operation.json` changes because argparse lists every
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
emits the capability. This receipt makes no full-suite, installed-engine, live-harness, native UI, Rust or
release qualification claim.

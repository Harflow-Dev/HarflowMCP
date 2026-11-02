# Python contract and baseline migration checklist

Dispatchable work packages: [migration sessions](migration-sessions.md).

Status: planned, 2026-11-02. All work items remain unverified.

Own the authoritative Python engine and public contracts until explicit
transfer. Inputs are reproducible current behavior and reviewed v1 requirements;
outputs are the frozen baseline, contract fixtures, conformance scenarios and
pinned Python development runtime. Freeze does not require ticket closure or a
prescribed version bump.

## Dispatch and boundaries

Work only on the assigned item. Read the workspace AGENTS.md, this repository's AGENTS.md if present, and
applicable package instructions first; preserve concurrent edits and use primary checkouts.
The orchestrator delegates unit tests under the current workflow. A checkbox is
not authorization to start runtimes, mutate real roots, register clients, push,
release, clean up, or expand scope. Use disposable roots and workspaces where
needed. Report a blocker with concrete evidence; task status is not proof of
completion. Repository builds and tests must not depend on sibling checkouts.

The optional [workspace coordination index](../../HARFLOW_MIGRATION_CHECKLIST.md)
uses B0 (baseline), C0 (contracts and design), E0 (client and Electron on
Python), R0 (Rust parity), I0 (integrated stack), and O0 (owner cutover and
publication). It is not a build dependency. Freeze and qualify the reproducible
existing baseline without waiting for open feature tickets. Review v1 scope in a
parity ledger, including proposed sidebar additions, instead of assuming tickets
or UI polish are migration requirements without an accepted ledger decision. All boxes below are deliberately unchecked;
scaffolds alone do not establish completion. Early-use gates and defaults remain
subject to evidence and explicit authorization for root registration. Use fresh
Rust state; archive historical task, run and event records rather than importing
them. Import only definitions, configuration and preferences, with an explicit
registration backup and handoff. No engine may share a writable root.

Stage dependencies: B0 → C0 → E0 → R0 → I0 → O0.

## B0 — establish baseline

- [ ] B0-MC-01 Record current commits, runtime selection, fixture digest, ctl
      inventory, MCP schema and supported backend qualification evidence.
- [ ] B0-MC-02 Review a v1 parity ledger against specification and observed
      behavior; record discrepancies as defects rather than adopting them blindly.
- [ ] B0-MC-03 Qualify the reproducible baseline and document failures and
      limits without waiting for unrelated feature tickets.

## C0 — contract and conformance foundations

- [ ] C0-MC-01 Specify and fixture necessary additive event paging, status and
      read gaps, including bounded pages, cursor expiry, retention gaps, drain
      and change semantics.
- [ ] C0-MC-02 Specify version handshake and validate compatibility and unknown
      optional fields without breaking existing v1 consumers.
- [ ] C0-MC-03 Fixture ctl unknown/exit behavior and run/resume payloads; capture
      the included MCP tools schema and document deferred unsupported tools.
- [ ] C0-MC-04 Build independent black-box conformance scenarios from the
      reviewed contract with disposable roots and external oracles; run Python
      first and record manifest hash and baseline defects.
- [ ] C0-MC-05 Coordinate byte-identical fixture re-vendoring and compatibility
      checks with Mac before announcing a contract change complete.

## E0 / R0 / O0 — support migration

- [ ] E0-MC-01 Support all accepted client flows through public operations on
      the explicitly selected Python development runtime.
- [ ] R0-MC-01 Run the same conformance suite against Rust and adjudicate
      differences against the specification and reviewed baseline.
- [ ] O0-MC-01 Preserve archive and registration handoff evidence until the owner
      explicitly approves retirement and contract ownership transfer.

## Validation

Run `uv run pytest`, `scripts/check-private-refs.sh` and
`uv run python scripts/check-legacy-names.py` as appropriate; see
[development](development.md), [architecture](architecture.md) and
[contracts](contracts.md). Real CLI or paid integration qualification requires
separate authorization.

## Handoff evidence

For every assigned item report: item ID and result; repository and dependency
commit IDs; fixture manifest hash where applicable; commands, environment,
results and evidence locations; local, committed, pushed, CI-passed, packaged
and released state, stated separately; approved design commit, path and state where
applicable; deviations, blockers and remaining work. Record unrun checks and
why. Do not infer passing gates from a dispatched task's status.

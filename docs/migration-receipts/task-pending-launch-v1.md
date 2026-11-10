# Pending launch read capability producer receipt

This receipt covers the additive Python canonical producer for `task-pending-launch.v1`.
It does not transfer contract ownership, qualify a published release, or certify a native consumer.

## Scope and integration order

The producer adds direct-human-only `task-pending-page` and `task-pending-status` ctl operations,
capability negotiation and a qualified display projection. Inventory scope is
`prerecord_launch_attempts_and_not_started`; resource-free handshakes are discoverable with
unsupported cancellation mechanism. Existing cancellation, MCP, ordinary status/batch shapes,
launch admission and product version remain unchanged. Consumers must independently negotiate
the selected runtime, vendor the entire byte-identical final bundle, and implement the coverage,
full-ID and ordinary-record handoff rules in `docs/contracts.md` before UI adoption. Published
0.0.12 binaries are not certified by this source receipt.

The canonical bundle has 334 fixtures. Its manifest SHA256 is
`f86fbe421ac3dc467265e8ce77756f0b6b70b63c008edd13b4ac1ae03aa65ff9`.
The generator uses actual ctl serialization and qualified isolated launch/task documents, with
controlled direct caller, clock and opaque cursor identities. It includes all five in-scope raw
launch states, retained not-started reasons, sanitized source refusals, exact recorded/missing/
inconsistent outcomes, paging/bounds, and unchanged existing status/batch pending data. Controlled
cursor identity does not replace the real persisted path/inode qualification in the specimen setup.
Conformance manifest count/digest pins move with this complete bundle; older negative handshake
specimens preserve their intentionally missing capability.

## Bounds and authority

Each namespace enumeration covers at most 65,536 all-directory entries and 8 MiB of filename
bytes. Complete ordinary-caller and launch namespaces are each checked before/after reads, at
most four bounded scans per page, sharing the request deadline and metadata allowance. No
partial ordinary-caller candidate list may authorize a human; bounds/refusals stay undecidable.
The existing detector receives complete qualified candidates through an optional takeover seam,
whose default call behavior is unchanged. Each page examines
at most 100 candidates, and returns at most 1..100 rows (50 by default). Metadata has a cooperative
5-second / 8-MiB allowance, and serialized output is capped below 128 KiB with reserved envelope
space. Namespace bounds or unqualified rechecks fail with unknown coverage; page read/response
limits preserve the last completed candidate. No-progress bounds fail explicitly. Namespace hashes
are filename revisions, not content generations or atomic snapshot claims. Cursors are unsigned
positions and never certificates of previous traversal or authority. Clients must traverse fresh
from page one and retain every uncertainty before replacing incomplete inventory.

The existing direct-caller gate precedes launch/target inspection. Environment markers refuse
before selected-state inspection; state ownership uses `initialize=False`, so unavailable state
is never initialized. Qualified launch and ordinary-record readers execute within task operations.
No unbounded launch scan, owner-derived task view, liveness inference, repair, recovery, relaunch,
or private native persistence access is added. Pending metadata never grants mutation authority.

## Verification boundary

Verification uses a separate temporary Python 3.13 environment installed from the locked project
dependencies. The shared checkout's existing environment is untouched. The affected suite selection
covers pending reads, wire contracts, bounded read authority, Continue contracts, approval seams,
takeover, lineage and independent conformance manifest/runner tests. After the bounded caller-gate
review fix, the final affected selection passed 340 tests (60.94 seconds), with existing Python
fork and tarfile deprecation warnings. The final 50-test focused pending-read run also passed. Fake process tests require process inspection;
a sandboxed run failed when `/bin/ps` was denied, and an allowed rerun passed. These are isolated
synthetic checks, with no paid/live providers or installation. The producer fixture generator is
checked twice for deterministic canonical bytes. Legacy-name, private-reference and whitespace
guards are included. This receipt makes no full-suite, installed-engine, live-cancellation, native
UI, Rust or release qualification claim.

Primary verification independently passed the final 340-test affected selection (62.88 seconds), including takeover and lineage, with two existing deprecation warnings. The initial review identified an unbounded caller-history scan; the bounded lazy candidate fix passed the final reruns and second independent review closed clean. Fixture generation/check, legacy/private-reference guards and whitespace checks passed. No source changed after that final verification.

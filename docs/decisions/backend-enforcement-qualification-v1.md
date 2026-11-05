# Backend and OS-enforcement qualification v1

Candidate qualification design owned by HarflowMCP. Runtime implementation belongs
in Rust. This adds no Python runtime, supported wire capability, engine version,
consumer selection or public launch path. Existing owner-runtime-v1 and supported
contract manifests remain unchanged.

## Independent claims

Qualify filesystem enforcement, native tool permission/protocol behavior, network,
publication, process/descendant containment, caller authority and resource budgets
independently. A native sandbox flag, requested freedom, version string, seeded
Python receipt, synthetic specimen or successful result is not OS enforcement.
Unknown/failed/unsupported/stale evidence refuses its dependent operation. No
fallback to tool policy or unrestricted execution is permitted. Tool policy still
requires explicit opt-in and does not grant additional authority.

The first contribution exports diagnostic operation measurements only. It has no
positive production token, no conversion to the existing model Passed field and no
platform-wide strict-qualified boolean. Production strict execution stays unsupported.
It measures local macOS arm64 OS mechanisms and conditionally the installed
Claude no-tools protocol against a scripted local Anthropic fixture, without
credentials or paid provider calls. It is a bounded R05-related contribution, not full R05/R12/ST06 or
R03/R04 acceptance. Other backends remain explicitly unqualified for production
strict execution. No real provider success is inferred from a scripted fixture.

## Threat and topology boundaries

The OS claim concerns writes by the launched process and its actual descendants,
not a promise about a hostile kernel, administrator or outside actor. Host-side
exclusivity and grant topology are trusted admission prerequisites, not values an
agent or public JSON may assert. They require real caller/resource integration
before production admission; the current fixture engine has no such public gate.

A simple path-based sandbox is insufficient. On macOS26.6.2 arm64, actual unpaid
probes showed pre-existing hardlinks inside a writable root modifying a denied
outside file. Denying new links does not revoke existing aliases. Renaming an
allowed ancestor also relocated a protected descendant beyond its denied path.
Sandbox manual documents that inherited pre-opened writable descriptors may still
write after confinement. Preserve these as counterexamples, never pass them as
ordinary successful isolation checks.

The harness creates fresh disposable physical roots itself; it accepts no user
tree and no asserted exclusivity flag. For this constrained measurement route,
require physical root identities and bounded, non-following topology inventory. Refuse any
unqualified existing symlink, multiply-linked regular file, special file, unknown
metadata, changing root or inventory overflow. Deny new hardlinks in the OS profile.
Freeze rename/unlink of each protected ancestor through the writable root, while
retaining intended ordinary descendant file edits. This is a precondition plus OS
mechanism; a scan alone cannot prove ongoing exclusion or resist hostile external
replacement. Without trusted stability, report unsupported/unknown and do not launch.
No production route is admitted by the first measurement harness.

The fixed profile denies writes by default, allows only exact requested workspace
and separate runtime scratch roots, protects state/configuration/registration/Git
control trees, and denies parent signaling and unqualified delegation (Mach lookup,
Apple events, external/local network except the exact scripted fixture endpoint).
Read/data confidentiality is separately declared: actual probes must not read
user credentials/state; isolated HOME/config alone is not a read sandbox. Kernel,
SQLite/library and startup OS calls are not given a universal wall-time guarantee.
Deprecated sandbox-exec availability is part of exact platform evidence, not a
future portability guarantee. Unsupported platforms refuse without weaker fallback.

## Identity and evidence

Bind every result to exact backend version, logical launch selection plus observed
physical executable identity/hash, interpreter/package closure when applicable,
OS version/build/architecture, sandbox executable hash, profile/compiler/probe
revision, effective argv/environment/configuration digest, grant digest, requested
write roots/protected targets and their physical identities, topology/exclusivity
provenance, and exact observation scopes. Never persist secrets or use raw auth
material as report content. Use selected Claude launch path without replacing its
symlink semantics; identity hashing may observe its physical target independently.
A launcher-only hash is not full runtime identity.

Keep denied action success/failure and resulting external filesystem evidence
separate. Require allowed positive controls so a broken helper cannot pass every
denial. Revalidate identities/configuration before/after observation and before any
future admitted effect. Late, changed, incomplete or over-budget results remain
unknown. Persisted/exported report JSON is diagnostic data and cannot reconstruct
a trusted admission token; only the live trusted adapter/composition can create one.
There is no positive production token constructor in this contribution.

## Actual probe matrix

Use disposable physical roots only. Cover direct create/truncate/append/unlink/
rename/chmod, protected nested roots and ancestors, symlinks, pre-existing and new
hardlinks, descendants/shell/alternate executable, nested sandbox attempts,
pre-opened writable FD positive control versus real safe-spawn FD exclusion,
parent control and delegation/network refusal. Observe external byte/inode/root
witnesses rather than trusting helper text or exit zero. Unsupported operation or
missing positive control cannot become pass. Exact grant and profile changes
invalidate the relevant observation, never silently reuse a broad platform pass.

The installed Claude route is a distinct private no-tools, no-fork fixture profile,
not the ordinary inherited Python adapter or production freedom. Before any vendor
invocation, independently measure process-fork and posix_spawn denial and a read
boundary excluding actual user credentials/state. The original vendor PID is then
the only allowed process; threads die with it. If startup requires a subprocess or
unqualified managed hook, report unsupported rather than relaxing the profile.
Controlled helper descendant probes use a separately identified profile, audited
fixed child programs and retained handles; they do not qualify arbitrary daemon or
setsid containment. All profiles/results remain separately identified.

Safe spawn continues using a fixed physical system executable and the parent's
unchanged current directory. Use sandbox-exec followed by the observed system
/usr/bin/env -C disposable-repo and then the selected logical Claude launch path.
Only the child changes cwd; the safe-spawn API and parent process never chdir.
Record wrapper/env binary hashes and selected symlink/physical target bookends.
Those observations do not rule out hostile executable replacement races and do not
become a production runtime identity token. Do not resolve the selected vendor
argument into a different pinned launch path.

The installed Claude check uses explicit isolated HOME/config/runtime paths,
no inherited credentials/proxies/startup variables, disabled user/project settings,
hooks/plugins/MCP and exact fake credentials/loopback provider. The local fixture listener owns an ephemeral 127.0.0.1 port exclusively and
uses one-use nonce-bound routes. Before the provider phase it permits exactly one
`GET /<nonce>/preflight` positive-control exchange. The provider phase permits only
the expected `POST /<nonce>/v1/messages`, bounded bodies/request count, and fixed
bounded SSE script; redirects, unexpected requests and alternate endpoint fallback
fail. Preflight must actually observe denied fork/posix_spawn and outside-read
operations, an allowed inside-read control, and allowed-versus-denied listener
destinations under the exact same profile before any vendor process starts. Measure exact destination admission
against an independently owned denied loopback endpoint before vendor startup.
Only the no-tools message/result protocol is in the initial vendor scope; actual
native tool permission and full production backend features remain unqualified.
Outer OS syscall results are reported independently. Start it only after OS probe safety
is established. Any external connection/config fallback refuses, not a paid retry.
No source build or test depends on the sibling Python checkout.

## Bounds and settlement

One measurement at a time, bounded topology entries/depth, bounded file sizes and
hash work, finite output/line/HTTP/SSE budgets, exact one-use fixture nonce and
request counts, explicit wall deadlines and process/thread/FD bounds. Source
fixtures may use larger stated measurement budgets than the old four-second effect;
these are never production execution defaults. All actual children retain original
wait authority and are settled with their server/reader threads before temporary
root teardown. Vendor startup is gated by proven no-fork behavior, so a process
group is never used as evidence that unknown vendor descendants are gone. Unknown observation is never death; on unresolved cleanup retain
resources or abort the isolated fixture before deleting roots. No unbounded join,
PID-only control, descendant adoption, arbitrary sleeps for ordering or replay.

## Delivery and remaining gates

Review this design and implementation scopes first. Commit only this disjoint
canonical specification locally, then pin its exact source/hash in Rust. Add actual
measurement adapters and dedicated regressions, independently verify, review and
commit one cohesive Rust stage. Preserve tracker-owned files and all existing pins.
No push, release, install, registration or live/paid provider call is authorized by
this design. Authenticated provider, production caller/exclusivity, containment,
production budgets, other platforms and actual storage faults remain distinct gates.

# Harflow public wire contracts

HarflowMCP 1.0.0 is unreleased. This specification covers ctl, setup, normalized events and version-info. Persisted task, workflow, run and artifact documents retain their existing formats; storage ownership and migration are separate work. Raw vendor streams, MCP negotiation, cursors and internal indexes are unchanged.

## Common envelope

Every wire document contains `product: "harflow"`, a format discriminator, integer `v: 1`, and `product_version: "1.0.0"`. Booleans are not integer versions. The formats are `harflow.ctl`, `harflow.setup`, `harflow.event` and `harflow.version-info`.

Validate identity and format before interpreting either a payload or an error. Missing or foreign product/format, unsupported versions and malformed product versions are rejected. Unknown optional fields and unknown normalized event kinds are additive. Payload fields cannot override the reserved common envelope fields.

Product versions are strict numeric `major.minor.patch`, with no leading zeroes. Each component is at most 2147483647. Optional dot-separated ASCII alphanumeric/hyphen build metadata after `+` is accepted and ignored for compatibility. Prereleases are unsupported. The complete version string is at most 256 bytes.

Live ctl, setup and version-info documents require matching major and minor components; patches in either direction are accepted. Validate each live response independently after the handshake. For persisted events, including events embedded in ctl pages, `product_version` is writer provenance: validate its syntax but do not require the current release line. Event product, format and integer wire version remain mandatory. A historical event from 1.0.7 remains readable by a consumer of event v1 running 1.1.0, while a live 1.0.7 engine fails that consumer's handshake.

## ctl and setup

`--json` emits one JSON document. ctl command-specific payloads and error objects retain their existing meanings, including batch `tasks`/`errors`. ctl usage errors contain `error.code: "usage"` and exit 2.

Setup results retain `server_path` and per-client `clients` rows. Client failures stay in those rows. With `--json`, process-level setup failures contain `error: {code: "setup_error", message: ...}` and exit 1; usage failures use `code: "usage"` and exit 2. Plain-text mode retains stderr and exit behavior. A nonzero process without a valid JSON envelope is a transport failure, not an empty successful result.

## Version-info and operation handshake

`harflow-ctl version-info --json` uses the thin lazy installed entrypoint. Success and invalid-usage paths import no ctl, task, backend, server, store or client runtime, read no configuration/history, probe no harness and create no files or directories, including with stale caller environment variables. Invalid usage emits a `harflow.version-info` envelope with `error.code: "usage"` and exits 2.

The payload has `component: "engine"`, `contracts` entries named `ctl`, `setup` and `events` with their exact `format`/integer `v`, and `capabilities: ["ctl.v1", "setup.v1", "events.v1", "version-info.v1"]`. Missing required capabilities fail the corresponding operation; unknown additive capabilities are tolerated. `os` is Python's platform identifier (`darwin` or `linux`, for example); `architecture` reports the actual machine architecture. App integration requires `darwin`/`arm64`; reporting other platforms does not remove Linux engine support.

`state_root` is the absolute lexically normalized effective child `HOME/.harflow`. Normalization removes dot/dotdot components and trailing separators, preserves case and never resolves symlinks. No state-root override or importer is introduced. The root need not exist. Compare roots case-sensitively; aliases that differ lexically refuse safely.

Consumers perform one fresh version-info probe immediately before each ordinary ctl or setup operation, with the same effective HOME, environment and working-directory policy. Setup probes the paired ctl from the selected runtime. Validate product line, required formats/capabilities, platform and selected state-root equality before launching the requested command. Historical ctl without version-info is incompatible. Approval is not cached across operations. This handshake does not establish authenticity or immutable runtime identity, and does not permit concurrent incompatible writers to share a store. Production managed runtime integration remains unavailable.

## Normalized events and budgets

Every persisted normalized event line and every projected event object retains the event envelope. Known and unknown additive event kinds share the same identity checks. Oversize replacement stubs also retain the envelope and writer provenance. Common discriminator bytes count toward the existing 16384-byte event and 262144-byte page budgets; limits are not increased. Raw vendor event logs are not rewritten or stamped.

## Canonical fixtures

[contracts/v1](../contracts/v1/manifest.json) is owned by the engine repository. Each fixture is canonical UTF-8 JSON or single-line JSONL with a trailing newline. `manifest.json` lists exact file SHA256 values and expected validation cases; `manifest.sha256` is the SHA256 of those exact manifest bytes. The fixture bundle digest is that manifest digest. Negative cases intentionally contain incompatible envelopes.

Swift vendors identical bytes in its test resources with provenance and the same digest. Each repository builds independently without sibling lookup or network fixture fetching. The fixtures include patch compatibility in both directions, historical inner event provenance, unknown fields/kinds and required capability failures. The bundle is compatibility test evidence, not release artifact pinning.

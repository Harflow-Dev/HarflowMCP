# Changelog

## Unreleased

- Mark fresh engine stores and enforce compatible shared ownership across processes, operations and background writers.
- Refuse nonempty unmarked or incompatible stores with explicit `state_incompatible` errors; report bounded ownership contention as `state_busy`.

- Centralize engine state-root selection and propagate `HARFLOW_STATE_ROOT` to agents and workflow supervisors.
- Refuse invalid selected roots at startup and unsupported injected task layouts before launch.
- Normalize relative explicit state paths and relative HOME defaults to absolute lexical paths against the working directory.

- Define explicit Harflow v1 ctl, setup and normalized event envelopes with product identity.
- Add side-effect-free version-info compatibility reporting and canonical cross-language fixtures.
- Emit structured setup process and usage errors when JSON output is requested.

- Extract the engine into HarflowMCP with independent package, command, environment, and state identities.
- Preserve current wire-format versions during extraction.
- Require requalification before enabling renamed native workflow adapters or extra writable path grants.

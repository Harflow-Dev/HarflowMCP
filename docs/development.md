# Development

Install the locked development dependencies and run the unit suite and repository guards:

```sh
uv sync --locked
uv run pytest
scripts/check-private-refs.sh
uv run python scripts/check-legacy-names.py
```

Paid integration tests require explicit opt-in. The default unit suite makes no paid model calls. CLI integration checks also require explicit opt-in and depend on installed client binaries; consult the test markers in [pyproject.toml](../pyproject.toml) before running them.

## Current qualification limits

Native Claude and Codex workflow adapters and extra writable path grants remain unavailable pending no-paid requalification of the renamed product. Ordinary headless agent dispatch remains supported. Historical qualification details in the guides do not certify HarflowMCP.

The companion Harflow macOS app prepares and qualifies its exact paired engine in app-independent managed storage; see its [managed runtime guide](https://github.com/Harflow-Dev/HarflowMac/blob/main/docs/managed-runtime.md). Local fixture validation does not establish PyPI package availability or signed distribution. Automatic app opening remains unavailable. Setting `HARFLOW_OPEN_MONITOR=1` does not enable app opening.

Permission features use the shared binary-bound qualification framework in `backends/qualification.py`. Claude and Codex retain historical measurements as seeds; an unseen version runs an isolated behavior probe automatically when a gated feature is requested. Refresh explicitly with `uv run harflow-ctl qualify claude --json` or `uv run harflow-ctl qualify codex --json`. Results are cached per binary hash, version, platform and probe revision; failures refuse the feature. Automatic probes currently require macOS `sandbox-exec`; unavailable isolation fails closed. Ordinary launches without gated features do not wait for qualification. `permission-preview --qualification-only` and `workflow-preview NAME --qualification-only` report only requested feature requirements from cached evidence, with no provider self-tests. An empty required set does not inspect the binary. See [the discovery contract](contracts.md#backend-qualification-discovery).

The existing no-paid opt-in suites remain broader developer regression checks. To test a candidate Claude version against an isolated localhost fake API, run:

```sh
HARFLOW_QUALIFY_CLAUDE_VERSION=<exact version> HARFLOW_CLI_INTEGRATION=1 uv run pytest -m cli_integration \
  tests/test_claude_denials_cli.py tests/test_claude_caller_approvals_cli.py \
  tests/test_claude_approval_resume_cli.py tests/test_claude_write_approvals_cli.py tests/test_claude_commands_cli.py
```

`allowed_commands` requires a passing seeded or automatically probed receipt and is rechecked at every launch, resume and continuation; a run that carries grants is also stopped, with a bridge notice, when its init event reports a different version, no version, or a tool event arrives before init. The grammar in `backends/claude_commands.py` is a guard against mistakes, not a security boundary, and path resolution happens at dispatch time only. Other backends except qualified Vibe ACP refuse the field.

Current limits of the qualification: caller approvals exist for Claude Code (2.1.310; 2.1.302 Bash-only; 2.1.312) and for Codex through `codex app-server` (0.163.0, macOS only; commands, and file changes with `caller_write_approvals`); Vibe 3.0.3/macOS qualifies ACP caller approvals (Bash/read; write/edit with the write opt-in), exact command grants and configured-model selection. `allowed_commands` is available for Claude Code and qualified Vibe. Not implemented: `allowed_commands` on Codex, caller approvals or grants on opencode, Antigravity, and Cursor, approvals for native workflow children, a caller-authorized `permission_grant` retry of a permission-blocked workflow node, approval notifications, and Monitor presentation of approvals and grants (HarflowMac issues 13 and 14). Cursor headless dispatch also needs its own `agent login` or `CURSOR_API_KEY`; the Cursor editor session is not that login.

The Codex counterpart runs real `codex app-server` sessions against the localhost fake Responses API with an isolated `HOME` and `CODEX_HOME` (no model calls, skipped unless codex-cli 0.163.0 on macOS), and checks the recorded protocol schema for drift:

```sh
HARFLOW_CLI_INTEGRATION=1 uv run pytest -m cli_integration tests/test_codex_app_server_cli.py tests/test_codex_app_server.py -k 'cli or execpolicy or schema'
```

Without the variable these tests skip unless the installed version is already in the table. These developer tests do not write production qualification receipts. Use `harflow-ctl qualify` to refresh the installed binary's production receipt; record broader measurements in [backend CLI facts](backend-cli-facts.md).

## Technical references

Tool docstrings in [server.py](../src/harflow_mcp/server.py) define the MCP surface. See [workflow context benchmarks](workflow-context-benchmarks.md) for deterministic fixture definitions and reproducible measurements, and [harness permissions](harness-permissions.md) for backend permission boundaries.

[Architecture](architecture.md) holds the maintainer-facing detail behind the rules in `AGENTS.md`: the backend and client seams, recovery and live input, takeover and ctl, enforcement claims, and caller reporting. [Backend CLI facts](backend-cli-facts.md) records the measured behavior of each agent and client CLI; read it before changing argv, flags, stdin wiring, stream parsing, classification or registration, and record new measurements there.

Backend invocations may contribute only their explicitly validated child environment. Vibe's headless invocation retains `VIBE_CLI=python` for older launchers; Vibe 3.0.1 ignores it and always launches Rust. Isolated loopback headless measurements cover Vibe 3.0.1, 3.0.2 and 3.0.3, not every intervening or older release. On 2026-11-03 all 11 measurement cases passed on 3.0.2, including structured refusal shapes. ACP, caller approvals, command grants and configured-model selection remain qualified only on 3.0.3; measuring headless behavior does not widen those production gates. Structured permission callback scopes were measured on 3.0.3. Interactive takeover inherits the user's environment. `read_only` uses `ask`, an approval policy without a read-only write-tool exclusion; inherited user/project policies or a custom profile may pre-approve writes. Loopback fake-provider CLI qualification covers start, resume, large stdin, approval denial and inherited-policy write approval, turn cap and provider usage-limit failure without paid calls (see `tests/test_vibe_cli.py`).

## Ordinary Codex exec qualification

Current macOS codex-cli 0.164.0 qualification uses isolated HOME/CODEX_HOME, synthetic repositories
outside implicit temporary write roots, and a loopback fake Responses API, without credentials or
paid model calls. It demonstrates trust persistence, inherited user/trusted-project rule bypasses,
project layering and MCP startup; direct `codex sandbox` measurements alone do not qualify ordinary
exec authority. The selected policy preserves launch/configuration behavior and discloses trust
persistence and uncertain effective authority through conservative reports. Qualified bounded modes
keep their existing version and configuration gates; no additional mode is enabled by this correction. See
[measured facts](backend-cli-facts.md#ordinary-codex-exec-authority).

## Shared interaction regression checks

Shared caller/Monitor answering is covered by `test_workflow_shared_interactions.py`,
`test_workflow_interactions.py`, `test_workflow_input_tree.py` and
`test_g03_approval_responder.py`. Native notification discovery is exercised through
`test_interaction_events.py` against isolated retained state; these tests never post
macOS notifications or send provider messages. Contract fixtures include explicit
synthetic shared-answer specimens and actual isolated approval CLI output.

## Isolated conformance infrastructure

`conformance/` is the S07 black-box foundation; see its
[maintained guide](../conformance/README.md) for explicit source/binary selection,
manifest pinning, isolated roots and report validation. Run its independent
self-checks with `.venv/bin/python -m pytest conformance/tests -q`. They do not
load the engine test tree's autouse fixtures, require providers or install agents.
macOS process observation may require access beyond a filesystem sandbox; blocked
observations fail closed. The guide's source check invokes only public
version-info/state-inspect operations on a fresh missing root. Standalone fake
execution does not qualify engine-to-fake integration, task/authority behavior,
workflows, a managed runtime or a real harness; those need their assigned later
sessions and authorization. Runtime hashes describe selected observed bytes, not
publisher authenticity or a complete OS/stdlib dependency closure.

### Pending launch read qualification

`task-pending-launch.v1` is an additive, direct-human ctl read capability in source, independently
negotiated from the selected engine. The isolated producer suite
`tests/test_task_pending_reads.py` and `scripts/generate-pending-launch-fixtures.py --check`
exercise qualified stored launch/task documents, source refusals, bounded namespace traversal,
read/serialization budgets and actual ctl serialization without starting providers. The generator
also captures existing single/batch `task_pending_launch` data unchanged. These synthetic checks
do not qualify installed binaries, process cancellation, paid providers, the native consumer or
Rust adoption. See [the contract](contracts.md#pending-launch-discovery-task-pending-launchv1)
for coverage and cursor limits. The complete canonical fixture bundle must be re-vendored before
consumer adoption; a current source capability must not be inferred from product version alone.

### Backend model listing qualification

`backend-models.v1` is an additive ctl read capability in source, negotiated independently of the product
version. `tests/test_bounded_process.py` covers the two-pipe runner (deadline kill of a whole process group,
output ceilings, spawn failure, stdin from `/dev/null`); `tests/test_backend_models.py` covers each backend's
source against recorded outputs under `tests/data/backend_models` and fake binaries (every state and reason,
bounds, effort intersection, file-source hardening, and that a harness that is not installed spawns and reads
nothing); `tests/test_ctl_backend_models.py` covers the caller gate, the shared 10 second budget, config-only
dispatch and the printed-line bound through `ctl.main`. `scripts/generate-backend-models-fixtures.py --check`
and `validate_backend_models` in `conformance/oracles.py` cover the fixtures. These checks use fake harnesses
only: they do not qualify an installed CLI version, a logged-in Cursor account, the native consumer or Rust
adoption. Re-measure and update `docs/backend-cli-facts.md` when a harness changes its listing output. Run the
fixture generators in this order after any change that touches the version-info capability list: steering,
approval, handshake, pending-launch, the remaining generators, then backend-models; then run every generator
with `--check`.

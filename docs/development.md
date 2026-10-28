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

Permission features use the shared binary-bound qualification framework in `backends/qualification.py`. Claude and Codex retain historical measurements as seeds; an unseen version runs an isolated behavior probe automatically when a gated feature is requested. Refresh explicitly with `uv run harflow-ctl qualify claude --json` or `uv run harflow-ctl qualify codex --json`. Results are cached per binary hash, version, platform and probe revision; failures refuse the feature. Automatic probes currently require macOS `sandbox-exec`; unavailable isolation fails closed. Ordinary launches without gated features do not wait for qualification.

The existing no-paid opt-in suites remain broader developer regression checks. To test a candidate Claude version against an isolated localhost fake API, run:

```sh
HARFLOW_QUALIFY_CLAUDE_VERSION=<exact version> HARFLOW_CLI_INTEGRATION=1 uv run pytest -m cli_integration \
  tests/test_claude_denials_cli.py tests/test_claude_caller_approvals_cli.py \
  tests/test_claude_approval_resume_cli.py tests/test_claude_write_approvals_cli.py tests/test_claude_commands_cli.py
```

`allowed_commands` requires a passing seeded or automatically probed receipt and is rechecked at every launch, resume and continuation; a run that carries grants is also stopped, with a bridge notice, when its init event reports a different version, no version, or a tool event arrives before init. The grammar in `backends/claude_commands.py` is a guard against mistakes, not a security boundary, and path resolution happens at dispatch time only. Other backends refuse the field.

Current limits of the qualification: caller approvals exist for Claude Code (2.1.310; 2.1.302 Bash-only; 2.1.312) and for Codex through `codex app-server` (0.163.0, macOS only; commands, and file changes with `caller_write_approvals`); `allowed_commands` only for Claude Code. Not implemented: `allowed_commands` on Codex, caller approvals or grants on Vibe, opencode and Antigravity, approvals for native workflow children, a caller-authorized `permission_grant` retry of a permission-blocked workflow node, approval notifications, and Monitor presentation of approvals and grants (HarflowMac issues 13 and 14).

The Codex counterpart runs real `codex app-server` sessions against the localhost fake Responses API with an isolated `HOME` and `CODEX_HOME` (no model calls, skipped unless codex-cli 0.163.0 on macOS), and checks the recorded protocol schema for drift:

```sh
HARFLOW_CLI_INTEGRATION=1 uv run pytest -m cli_integration tests/test_codex_app_server_cli.py tests/test_codex_app_server.py -k 'cli or execpolicy or schema'
```

Without the variable these tests skip unless the installed version is already in the table. These developer tests do not write production qualification receipts. Use `harflow-ctl qualify` to refresh the installed binary's production receipt; record broader measurements in [backend CLI facts](backend-cli-facts.md).

## Technical references

Tool docstrings in [server.py](../src/harflow_mcp/server.py) define the MCP surface. See [workflow context benchmarks](workflow-context-benchmarks.md) for deterministic fixture definitions and reproducible measurements, and [harness permissions](harness-permissions.md) for backend permission boundaries.

[Architecture](architecture.md) holds the maintainer-facing detail behind the rules in `AGENTS.md`: the backend and client seams, recovery and live input, takeover and ctl, enforcement claims, and caller reporting. [Backend CLI facts](backend-cli-facts.md) records the measured behavior of each agent and client CLI; read it before changing argv, flags, stdin wiring, stream parsing, classification or registration, and record new measurements there.

Backend invocations may contribute only their explicitly validated child environment. Vibe's headless invocation retains `VIBE_CLI=python` for older launchers; Vibe 3.0.1 ignores it and always launches Rust. Current qualification is Vibe 3.0.1, not a compatibility claim for every older release. Interactive takeover inherits the user's environment. `read_only` uses `ask`, an approval policy without a read-only write-tool exclusion; inherited user/project policies or a custom profile may pre-approve writes. Loopback fake-provider CLI qualification covers start, resume, large stdin, approval denial and inherited-policy write approval, turn cap and provider usage-limit failure without paid calls (see `tests/test_vibe_cli.py`).

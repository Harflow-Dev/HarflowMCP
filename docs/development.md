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

Claude caller approvals, denial classification and `allowed_commands` are gated by one measured table, `src/harflow_mcp/backends/claude_qualification.py`; currently 2.1.302 (Bash only, historical) and 2.1.310. To measure a new Claude Code version without changing what the product supports, run the no-paid opt-in suite against a localhost fake API with an isolated `HOME` and `CLAUDE_CONFIG_DIR`, naming the exact installed version:

```sh
HARFLOW_QUALIFY_CLAUDE_VERSION=<exact version> HARFLOW_CLI_INTEGRATION=1 uv run pytest -m cli_integration \
  tests/test_claude_denials_cli.py tests/test_claude_caller_approvals_cli.py \
  tests/test_claude_approval_resume_cli.py tests/test_claude_write_approvals_cli.py tests/test_claude_commands_cli.py
```

`allowed_commands` is qualified only on the table's measured versions (currently 2.1.310) and is re-qualified at every launch, resume and continuation; a run that carries grants is also stopped, with a bridge notice, when its init event reports a different version, no version, or a tool event arrives before init. The grammar in `backends/claude_commands.py` is a guard against mistakes, not a security boundary, and path resolution happens at dispatch time only. Other backends refuse the field.

Current limits of the qualification: caller approvals and `allowed_commands` exist only for Claude Code (2.1.310; 2.1.302 Bash-only for approvals). Not implemented: approvals or grants on Codex, Vibe, opencode and Antigravity, approvals for native workflow children, a caller-authorized `permission_grant` retry of a permission-blocked workflow node, approval notifications, and Monitor presentation of approvals and grants (HarflowMac issues 13 and 14).

Without the variable these tests skip unless the installed version is already in the table. A passing candidate run is evidence for a new table row, recorded in [backend CLI facts](backend-cli-facts.md); it does not itself qualify the version.

## Technical references

Tool docstrings in [server.py](../src/harflow_mcp/server.py) define the MCP surface. See [workflow context benchmarks](workflow-context-benchmarks.md) for deterministic fixture definitions and reproducible measurements, and [harness permissions](harness-permissions.md) for backend permission boundaries.

[Architecture](architecture.md) holds the maintainer-facing detail behind the rules in `AGENTS.md`: the backend and client seams, recovery and live input, takeover and ctl, enforcement claims, and caller reporting. [Backend CLI facts](backend-cli-facts.md) records the measured behavior of each agent and client CLI; read it before changing argv, flags, stdin wiring, stream parsing, classification or registration, and record new measurements there.

Backend invocations may contribute only their explicitly validated child environment. Vibe's headless invocation selects `VIBE_CLI=python`; interactive takeover inherits the user's environment.

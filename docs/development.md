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

## Technical references

Tool docstrings in [server.py](../src/harflow_mcp/server.py) define the MCP surface. See [workflow context benchmarks](workflow-context-benchmarks.md) for deterministic fixture definitions and reproducible measurements, and [harness permissions](harness-permissions.md) for backend permission boundaries.

[Architecture](architecture.md) holds the maintainer-facing detail behind the rules in `AGENTS.md`: the backend and client seams, recovery and live input, takeover and ctl, enforcement claims, and caller reporting. [Backend CLI facts](backend-cli-facts.md) records the measured behavior of each agent and client CLI; read it before changing argv, flags, stdin wiring, stream parsing, classification or registration, and record new measurements there.

Backend invocations may contribute only their explicitly validated child environment. Vibe's headless invocation selects `VIBE_CLI=python`; interactive takeover inherits the user's environment.

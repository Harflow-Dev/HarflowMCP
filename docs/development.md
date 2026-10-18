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

Harflow macOS integration and automatic app opening are unavailable until the companion runtime is integrated. Setting `HARFLOW_OPEN_MONITOR=1` does not enable app opening.

## Technical references

Tool docstrings in [server.py](../src/harflow_mcp/server.py) define the MCP surface. See [workflow context benchmarks](workflow-context-benchmarks.md) for deterministic fixture definitions and reproducible measurements, and [harness permissions](harness-permissions.md) for backend permission boundaries.

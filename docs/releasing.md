# Engine releases

Release automation builds and checks a wheel and sdist once. A published GitHub release in `Harflow-Dev/HarflowMCP` must use a strict `vX.Y.Z` tag matching `pyproject.toml` and `server.json`; its event SHA, tag commit and checkout must agree. A GitHub prerelease designation is supported without a prerelease version suffix. Manual dispatch validates and uploads workflow artifacts only.

Before publication, the owner must configure PyPI package-name ownership and the pending Trusted Publisher for this repository's `release.yml`, environment `pypi`, plus that protected environment. GitHub OIDC must authorize the case-sensitive registry namespace `io.github.Harflow-Dev/harflow-mcp`. These account prerequisites have not been verified by local validation. No long-lived PyPI token is used. Engine publication must precede a production Mac first install that requests this version.

Local validation (Python 3.11+, uv 0.9.26):

```sh
uv sync --locked
uv run --locked python -I scripts/release.py source
uv build --no-create-gitignore --build-constraints release-build-constraints.txt
uv run --locked python -I scripts/release.py artifacts
uv run --locked python -I scripts/release.py smoke --file dist/harflow_mcp-0.0.5-py3-none-any.whl
```

Use a fresh `dist` directory. Build dependencies are pinned in `release-build-constraints.txt`; the sdist wheel rebuild checks package resources independently. Smoke checks install only into temporary environments, run version-info and a stdio initialize/list-tools handshake, and invoke both `uvx harflow-mcp@0.0.5` and `uvx harflow-mcp` against an offline wheel directory. Dependency acquisition uses public PyPI; no model calls or client registration occurs. This verifies the registry-client command form, not arbitrary client compatibility. The `harflow-mcp` entrypoint aliases the existing server; `harflow-server` remains available.

The PyPI stage consumes the checked bytes, never rebuilds. Registry publication waits for the release-specific PyPI endpoint to expose the exact wheel with matching size and SHA256. It then validates metadata before GitHub OIDC login. Publisher v1.8.1's linux-amd64 archive SHA256 is pinned and was cross-checked against the official `registry_1.8.1_checksums.txt`; third-party Actions use verified immutable upstream commits. Manual validation has no OIDC or publishing stage. Local checks do not establish successful PyPI/registry publication or owner authority.

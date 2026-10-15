"""Protect added authority against registered backends' execution configuration."""
from pathlib import Path

from .canonical_paths import canonical


def assert_execution_config_outside_writes(repo: Path, grants, scratch_directory: str | None = None) -> None:
    from . import backends
    from .runtime_paths import _chain
    repo = canonical(repo)
    scopes = [(repo, 'directory'), *((Path(g.path), g.kind) for g in grants)]
    if scratch_directory is not None:
        scopes.append((canonical(Path(scratch_directory)), 'directory'))
    for backend in backends.BACKENDS.values():
        namespaces = backend.execution_configuration_namespaces(repo)
        for category in ('external', 'project'):
            for lexical in namespaces[category]:
                lexical = Path(lexical)
                if not lexical.is_absolute():
                    raise ValueError("backend execution configuration requires absolute home and namespaces")
                chain, target = _chain(lexical)
                inherited = category == 'project' and lexical.is_relative_to(repo) and target.is_relative_to(repo)
                for scope, kind in scopes:
                    overlap = (scope.is_relative_to(lexical) or scope.is_relative_to(target)
                               or any(scope == part or (kind == 'directory' and part.is_relative_to(scope)) for part in chain))
                    if not overlap:
                        continue
                    # No authority expands when a caller redundantly grants a
                    # repository ancestor of its own latent project config.
                    same_authority = inherited and kind == 'directory' and scope.is_relative_to(repo) and not scope.is_relative_to(lexical) and not scope.is_relative_to(target)
                    if not same_authority:
                        raise ValueError('writable authority overlaps backend execution configuration namespace')
                if lexical.exists() or lexical.is_symlink():
                    raise ValueError('existing backend execution configuration is not qualified for extra writable paths')

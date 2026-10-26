"""Keep bounded worker writes outside bridge authorization and control state."""
from pathlib import Path

from .canonical_paths import canonical


def _canonical_or_none(path: Path) -> Path | None:
    """The canonical spelling of an existing namespace; a namespace not created yet matches nothing."""
    try:
        return canonical(path)
    except FileNotFoundError:
        return None


def assert_state_outside_writes(log_dir: Path, repo: Path, grants, scratch_directory: str | tuple[str, ...] | list[str] | None = None) -> None:
    """Protect default CLI state and this registry's custom state before execution.

    Only the separately allocated scratch paths are writable within state: the task scratch, and
    the shared chain and group folders. Caller-supplied grants and repositories never receive that
    exception. `scratch_directory` is one path or several.
    """
    scratch_paths = [scratch_directory] if isinstance(scratch_directory, str) else list(scratch_directory or ())
    from .runtime_paths import _chain
    from .state_layout import protected_roots
    roots = protected_roots(log_dir=log_dir)
    scopes = [(canonical(Path(repo)), 'directory')]
    scopes.extend((Path(grant.path), grant.kind) for grant in grants)
    protected = []
    controls = []
    for root in roots:
        chain, target = _chain(root)
        protected.append((root.absolute(), target, chain))
        for name in ('tasks', 'sessions', 'workflows', 'workflow-runs', 'workflow-leases', 'workflow-owners', 'notifications.sqlite3', 'ctl.log'):
            child = root / name
            child_chain, child_target = _chain(child)
            protected.append((child.absolute(), child_target, child_chain))
            controls.append(child_target)
    # The task directory may have a separate alias/target outside its parent.
    task_chain, task_target = _chain(Path(log_dir))
    protected.append((Path(log_dir).absolute(), task_target, task_chain))
    for scope, kind in scopes:
        for lexical, target, chain in protected:
            if (scope.is_relative_to(lexical) or scope.is_relative_to(target)
                    or any(scope == path or (kind == 'directory' and path.is_relative_to(scope)) for path in chain)):
                raise ValueError('writable authority overlaps protected bridge task/control state')
    for scratch_path in scratch_paths:
        allocated = Path(scratch_path)
        base = Path(log_dir).parent
        # The scratch identity is verified in the kernel's actual spelling, so
        # a case-alias cannot present an allocated directory under a spelling
        # the namespace checks below would not recognize.
        verified = canonical(allocated)
        namespaces = [base / 'scratch', base / 'shared-scratch' / 'chain', base / 'shared-scratch' / 'group']
        if (any(namespace.is_symlink() for namespace in (*namespaces, base / 'shared-scratch'))
                or all(verified.parent != _canonical_or_none(namespace) for namespace in namespaces)
                or verified != allocated or allocated.is_symlink()):
            raise ValueError('task scratch identity is outside its allocated namespace')
        # Scratch must never resolve into the task-record tree or another state
        # root's controls via a substituted parent alias.
        if any(verified.is_relative_to(target) for target in (*controls, task_target)):
            raise ValueError('task scratch overlaps bridge task/control state')
